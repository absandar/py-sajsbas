#include <windows.h>

int WINAPI WinMain(HINSTANCE hInstance, HINSTANCE hPrevInstance, LPSTR lpCmdLine, int nCmdShow) {
    // Ejecuta el script sin mostrar consola
    ShellExecute(NULL, "open", "python", "controlador_servidor.py", NULL, SW_HIDE);
    return 0;
}
