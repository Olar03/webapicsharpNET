using MySql.Data.MySqlClient;
using System;

namespace webapicsharp.SServicios.Utilidades
{
    public static class ManejadorErroresMariaDB
    {
        public static Exception Procesar(MySqlException sqlEx, string nombreTabla)
        {
            return sqlEx.Number switch
            {
                1045 => new UnauthorizedAccessException(
                    "Acceso denegado: credenciales incorrectas en MariaDB.", sqlEx),

                1049 => new InvalidOperationException(
                    "La base de datos especificada no existe en MariaDB.", sqlEx),

                1146 => new InvalidOperationException(
                    $"La tabla '{nombreTabla}' no existe en MariaDB.", sqlEx),

                1064 => new InvalidOperationException(
                    "Error de sintaxis SQL en la consulta enviada a MariaDB.", sqlEx),

                2002 => new InvalidOperationException(
                    "No se puede conectar al host de MariaDB. Verifica que el servidor esté en ejecución y accesible.", sqlEx),

                _ => new InvalidOperationException(
                    $"Error de MariaDB al ejecutar operación sobre la tabla '{nombreTabla}': {sqlEx.Message}", sqlEx),
            };
        }
    }
}