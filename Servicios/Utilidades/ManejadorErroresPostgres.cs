using Npgsql;
using System;

namespace webapicsharp.Servicios.Utilidades
{
    /// <summary>
    /// Manejador centralizado de errores para PostgreSQL.
    /// Convierte excepciones Npgsql en mensajes más claros.
    /// </summary>
    public static class ManejadorErroresPostgres
    {
        public static Exception Procesar(NpgsqlException ex, string operacion, string? tabla = null)
        {
            string mensajeBase = $"Error en la operación '{operacion}' sobre PostgreSQL.";

            return ex.SqlState switch
            {
                // Tabla no existe
                "42P01" => new InvalidOperationException($"{mensajeBase} La tabla '{tabla}' no existe.", ex),

                // Columna no existe
                "42703" => new InvalidOperationException($"{mensajeBase} Una de las columnas referenciadas no existe en la tabla '{tabla}'.", ex),

                // Violación de clave primaria o única
                "23505" => new InvalidOperationException($"{mensajeBase} Violación de restricción única (duplicado).", ex),

                // Violación de clave foránea
                "23503" => new InvalidOperationException($"{mensajeBase} Violación de restricción de clave foránea.", ex),

                // Error de sintaxis SQL
                "42601" => new InvalidOperationException($"{mensajeBase} Error de sintaxis en la consulta SQL.", ex),

                // Error de conexión
                "08001" or "08006" or "08004" => new InvalidOperationException($"{mensajeBase} No se pudo establecer conexión con la base de datos.", ex),

                // Otros códigos
                _ => new InvalidOperationException($"{mensajeBase} Código: {ex.SqlState}. Mensaje: {ex.Message}", ex)
            };
        }
    }
}