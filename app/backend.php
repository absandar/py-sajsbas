<?php
ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(E_ALL);

header("Content-Type: application/json; charset=utf-8");

$apiKeyEsperada = "m8bdOmnm3uo8tt3Pfzi7iUAAKodiFOR3";
$apiKeyRecibida = $_SERVER['HTTP_PASS'] ?? '';

if ($apiKeyRecibida !== $apiKeyEsperada) {
    http_response_code(403);
    echo json_encode(["error" => "Acceso no autorizado"]);
    exit;
}

include("conexion.php");

// ==================================================
// === ASEGURAR TABLAS CON PREFIJO py_ ==============
// ==================================================
$link->query("
CREATE TABLE IF NOT EXISTS py_camaras_frigorifico (
    uuid VARCHAR(50) PRIMARY KEY,
    id_procesa_app INT NULL,
    fecha_de_descarga DATE,
    certificado VARCHAR(255),
    lote_basico VARCHAR(255),
    ubicacion VARCHAR(255),
    sku_tina VARCHAR(100),
    sku_talla VARCHAR(100),
    peso_bruto DECIMAL(10,2),
    tanque VARCHAR(100),
    hora_de_marbete VARCHAR(50),
    hora_de_pesado VARCHAR(50),
    fda VARCHAR(100),
    lote_fda VARCHAR(100),
    lote_sap VARCHAR(100),
    peso_neto DECIMAL(10,2),
    tara DECIMAL(10,2),
    observaciones TEXT,
    fecha_hora_guardado DATETIME,
    estado TINYINT,
    empleado INT
) ENGINE=InnoDB;
");

$link->query("
CREATE TABLE IF NOT EXISTS py_remisiones_general (
    uuid VARCHAR(50) PRIMARY KEY,
    folio VARCHAR(100),
    cliente VARCHAR(255),
    numero_sello VARCHAR(100),
    placas_contenedor VARCHAR(100),
    factura VARCHAR(100),
    observaciones TEXT,
    fecha_produccion DATE,
    borrado TINYINT DEFAULT 0,
    empleado INT,
    numero_remision INTEGER,
    fecha_creacion DATETIME
) ENGINE=InnoDB;
");

$link->query("
CREATE TABLE IF NOT EXISTS py_remisiones_cabecera (
    uuid VARCHAR(50) PRIMARY KEY,
    id_remision_general VARCHAR(50),
    carga VARCHAR(50),
    cantidad_solicitada DECIMAL(10,2),
    fecha_creacion DATETIME,
    borrado TINYINT DEFAULT 0,
    FOREIGN KEY (id_remision_general) REFERENCES py_remisiones_general(uuid)
) ENGINE=InnoDB;
");

$link->query("
CREATE TABLE IF NOT EXISTS py_remisiones_cuerpo (
    uuid VARCHAR(50) PRIMARY KEY,
    id_remision VARCHAR(50),
    sku_tina VARCHAR(100),
    sku_talla VARCHAR(100),
    tara DECIMAL(10,2),
    peso_neto DECIMAL(10,2),
    merma DECIMAL(10,2),
    lote VARCHAR(100),
    tanque VARCHAR(100),
    peso_marbete DECIMAL(10,2),
    peso_bascula DECIMAL(10,2),
    peso_neto_devolucion DECIMAL(10,2),
    peso_bruto_devolucion DECIMAL(10,2),
    is_msc TINYINT DEFAULT 0,
    is_sensorial TINYINT DEFAULT 0,
    observaciones TEXT,
    fecha_creacion DATETIME,
    borrado TINYINT DEFAULT 0,
    FOREIGN KEY (id_remision) REFERENCES py_remisiones_cabecera(uuid)
) ENGINE=InnoDB;
");

$link->query("
CREATE TABLE IF NOT EXISTS py_remisiones_retallados (
    uuid VARCHAR(50) PRIMARY KEY,
    id_remision_general VARCHAR(50),
    sku_tina VARCHAR(100),
    sku_talla VARCHAR(100),
    lote VARCHAR(100),
    tara DECIMAL(10,2),
    peso_bascula DECIMAL(10,2),
    peso_neto DECIMAL(10,2),
    observaciones TEXT,
    fecha_creacion DATETIME,
    borrado TINYINT DEFAULT 0,
    FOREIGN KEY (id_remision_general) REFERENCES py_remisiones_general(uuid)
) ENGINE=InnoDB;
");

// ==================================================
// === FUNCIONES AUXILIARES ========================
// ==================================================
function get_param_types_and_values($row, $ordenCampos)
{
    $types = "";
    $values = [];
    foreach ($ordenCampos as $campo) {
        $val = $row[$campo] ?? null;
        if (is_null($val)) {
            $types .= "s";
            $values[] = $val;
        } elseif (is_int($val)) {
            $types .= "i";
            $values[] = $val;
        } elseif (is_float($val) || is_numeric($val)) {
            $types .= "d";
            $values[] = $val;
        } else {
            $types .= "s";
            $values[] = $val;
        }
    }
    return [$types, $values];
}

function bind_dynamic($stmt, $types, $values)
{
    $refs = [];
    foreach ($values as $k => $v) {
        $refs[$k] = &$values[$k];
    }
    array_unshift($refs, $types);
    return call_user_func_array([$stmt, "bind_param"], $refs);
}

function table_columns($link, $table) {
    $cols = [];
    $res = $link->query("DESCRIBE `$table`");
    if (!$res) return $cols;
    while ($row = $res->fetch_assoc()) {
        $cols[] = $row['Field'];
    }
    return $cols;
}

function upsert_table($link, $table, $rows, &$processed, &$errors) {
    $tableCols = table_columns($link, $table);
    if (empty($tableCols)) {
        $errors[] = "Tabla no existe o no accesible: $table";
        return;
    }

    foreach ($rows as $row) {
        // conservar solo columnas que realmente existen en la tabla
        $filtered = array_intersect_key($row, array_flip($tableCols));
        if (empty($filtered)) continue;

        $campos = array_keys($filtered);
        $placeholders = implode(",", array_fill(0, count($campos), "?"));
        $insert_cols = implode(",", $campos);

        $update_parts = [];
        foreach ($campos as $c) {
            if (strtolower($c) === 'uuid') continue; // no sobrescribir PK
            $update_parts[] = "$c=VALUES($c)";
        }

        $sql = "INSERT INTO `$table` ($insert_cols) VALUES ($placeholders)";
        if (!empty($update_parts)) {
            $sql .= " ON DUPLICATE KEY UPDATE " . implode(",", $update_parts);
        }

        $stmt = $link->prepare($sql);
        if (!$stmt) {
            $errors[] = "Prepare failed for $table: " . $link->error;
            continue;
        }

        list($types, $values) = get_param_types_and_values($filtered, $campos);
        bind_dynamic($stmt, $types, $values);

        if ($stmt->execute()) {
            $processed++;
        } else {
            $errors[] = "Execute failed for $table: " . $stmt->error;
        }
        $stmt->close();
    }
}

// ==================================================
// === PROCESAR INPUT JSON =========================
// ==================================================
$input = file_get_contents("php://input");
$data = json_decode($input, true);

if (!$data) {
    http_response_code(400);
    echo json_encode(["error" => "JSON inválido"]);
    exit;
}

$respuestas = [
    "py_camaras_frigorifico" => 0,
    "py_remisiones_general" => 0,
    "py_remisiones_cabecera" => 0,
    "py_remisiones_cuerpo"   => 0,
    "py_remisiones_retallados" => 0
];

$errors = [];

// Mapeo de keys esperadas en el payload a tablas py_*
$allowed = [
    "camaras_frigorifico" => "py_camaras_frigorifico",
    "remisiones_general"  => "py_remisiones_general",
    "remisiones_cabecera" => "py_remisiones_cabecera",
    "remisiones_cuerpo"   => "py_remisiones_cuerpo",
    "remisiones_retallados" => "py_remisiones_retallados"
];

// Procesar cada conjunto de registros recibido
foreach ($allowed as $key => $table) {
    if (!empty($data[$key])) {
        upsert_table($link, $table, $data[$key], $respuestas[$table], $errors);
    }
}

// Respuesta
$response = [
    "status" => empty($errors) ? "ok" : "partial",
    "procesados" => $respuestas,
    "errors" => $errors
];

echo json_encode($response);
