#obsoleto
# Antes esta función decidía dinámicamente si se iba la nueva tina o se iba la vieja según cuál era más chica.
# Ahora el criterio cambió → siempre la nueva tina es la que se va a producción y la tina original (la que se divide) se queda en almacén.

# def dividir_tina(cantidad_solicitada, pesos_netos):
#     """
#     Determina qué tina se queda y cuál se va al dividir para cumplir un pedido.
    
#     Args:
#         cantidad_solicitada (int): Cantidad requerida
#         pesos_netos (list): Lista de pesos netos en el orden que fueron cargadas
    
#     Returns:
#         dict: Información sobre la operación realizada
#     """
    
#     # Calcular el total disponible
#     total_disponible = sum(pesos_netos)
    
#     # Verificar si hay suficiente cantidad
#     if total_disponible <= cantidad_solicitada:
#         return {"error": "No hay suficiente cantidad disponible"}
    
#     # Tomar siempre la última tina cargada
#     tina_a_dividir = pesos_netos[-1]
    
#     # Calcular el excedente
#     excedente = total_disponible - cantidad_solicitada
    
#     # Calcular el remanente después de la división
#     remanente = tina_a_dividir - excedente
    
#     # Decidir qué operación es más eficiente
#     if excedente <= remanente:
#         tina_se_queda = remanente
#         tina_se_va = excedente
#         operacion = "Transferir excedente a nueva tina"
#     else:
#         tina_se_queda = excedente
#         tina_se_va = remanente
#         operacion = "Transferir remanente a nueva tina"
    
#     return {
#         "cantidad_solicitada": cantidad_solicitada,
#         "total_disponible": total_disponible,
#         "excedente": excedente,
#         "tina_original": tina_a_dividir,
#         "remanente": remanente,
#         "tina_a_produccion": tina_se_queda,
#         "tina_a_almacen": tina_se_va,
#         "operacion_recomendada": operacion,
#         "tinas_utilizadas": len(pesos_netos)
#     }


def dividir_tina(cantidad_solicitada, pesos_netos):
    """
    Determina cómo dividir la última tina para cumplir un pedido.
    
    Reglas:
    - Siempre se divide la última tina cargada.
    - El excedente se coloca en una nueva tina.
    - La nueva tina siempre se va a producción.
    - La tina original (ya reducida) siempre se queda en almacén.
    
    Args:
        cantidad_solicitada (int): Cantidad requerida
        pesos_netos (list): Lista de pesos netos en el orden que fueron cargadas
    
    Returns:
        dict: Información sobre la operación realizada
    """
    
    # Calcular el total disponible
    total_disponible = sum(pesos_netos)
    
    # Validar disponibilidad
    if total_disponible < cantidad_solicitada:
        return {"error": "No hay suficiente cantidad disponible"}
    if total_disponible == cantidad_solicitada:
        return {"mensaje": "No es necesario dividir ninguna tina"}
    
    # Última tina a dividir
    tina_a_dividir = pesos_netos[-1]
    
    # Excedente que hay que devolver
    excedente = total_disponible - cantidad_solicitada
    
    # Remanente en la tina original después de dividir
    remanente = tina_a_dividir - excedente
    
    # Reglas nuevas: 
    # - La nueva tina (con `remanete`) siempre va a producción
    # - La tina dividida (con `excedente`) siempre queda en almacén
    return {
        "cantidad_solicitada": cantidad_solicitada,
        "total_disponible": total_disponible,
        "excedente": excedente,
        "tina_original": tina_a_dividir,
        "remanente_en_tina_original": remanente,
        "tina_a_produccion": remanente,
        "tina_a_almacen": excedente,
        "operacion_recomendada": "Nueva tina siempre se va a producción",
        "tinas_utilizadas": len(pesos_netos)
    }
