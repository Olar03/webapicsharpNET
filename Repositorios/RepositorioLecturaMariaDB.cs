
// RepositorioLecturaSqlMariaDB.cs — Implementación concreta para leer datos usando ADO.NET y MySQL
// Ubicación: Repositorios/RepositorioLecturaSqlMariaDB.cs
//
// Principios SOLID aplicados:
// - SRP: Esta clase solo se encarga de leer datos desde MySQL, nada más
// - DIP: Implementa IRepositorioLecturaTabla (abstracción) y usa IProveedorConexion
// - OCP: Si mañana se necesita PostgreSQL, se crea otra implementación sin tocar esta
// - LSP: Es completamente intercambiable con cualquier otra implementación de IRepositorioLecturaTabla

using System;                                          // Para excepciones y tipos básicos del sistema
using System.Collections.Generic;                      // Para List<> y Dictionary<> genéricos
using System.Threading.Tasks;                          // Para programación asíncrona con async/await
using MySql.Data.MySqlClient;                     // Para conectar y ejecutar comandos en SQL Server
using webapicsharp.Repositorios.Abstracciones;        // Para implementar la interfaz IRepositorioLecturaTabla
using webapicsharp.Servicios.Abstracciones;           // Para usar IProveedorConexion y obtener cadenas de conexión
using webapicsharp.Servicios.Utilidades;
using webapicsharp.SServicios.Utilidades;

namespace webapicsharp.Repositorios
{
    public class RepositorioLecturaMariaDB : IRepositorioLecturaTabla
    {

        private readonly IProveedorConexion _proveedorConexion;

        public RepositorioLecturaMariaDB(IProveedorConexion proveedorConexion)
        {
            // Validación defensiva: asegurar que la inyección de dependencias funcionó correctamente
            // Esta validación protege contra errores de configuración en Program.cs
            // Si proveedorConexion es null, significa que el registro de servicios está mal configurado
            _proveedorConexion = proveedorConexion ?? throw new ArgumentNullException(
                nameof(proveedorConexion),
                "IProveedorConexion no puede ser null. Verificar registro de servicios en Program.cs."
            );
        }

        public async Task<IReadOnlyList<Dictionary<string, object?>>> ObtenerFilasAsync(
          string nombreTabla,    // Nombre de la tabla (requerido)
          string? esquema,        // Esquema de la tabla (opcional, puede ser null o vacío)
          int? limite           // Límite de registros (opcional, por defecto 1000)
            )
        {
            // ==============================================================================
            // FASE 1: VALIDACIONES DE ENTRADA
            // ==============================================================================
            if (string.IsNullOrWhiteSpace(nombreTabla))
                throw new ArgumentException("El nombre de la tabla no puede estar vacío.", nameof(nombreTabla));

            // ==============================================================================
            // FASE 2: NORMALIZACIÓN DE PARÁMETROS
            // ==============================================================================
            int limiteNormalizado = limite ?? 100;

            // ==============================================================================
            // FASE 3: CONSTRUCCIÓN DE CONSULTA SQL ESPECÍFICA DE SQL SERVER
            // ==============================================================================
            string consultaSql = $@"
                SELECT *
                FROM `{nombreTabla}`
                LIMIT {limiteNormalizado}";

            // ==============================================================================
            // FASE 4: PREPARACIÓN DE ESTRUCTURAS DE DATOS
            // ==============================================================================

            var Resultados = new List<Dictionary<string, object?>>();

            try
            {
                // ==============================================================================
                // FASE 5: OBTENCIÓN DE CONEXIÓN (APLICANDO DIP)
                // ==============================================================================
                string cadenaConexion = _proveedorConexion.ObtenerCadenaConexion();

                // ==============================================================================
                // FASE 6: CONEXIÓN A SQL SERVER
                // ==============================================================================
                using var conexion = new MySqlConnection(cadenaConexion);
                await conexion.OpenAsync();
                // ==============================================================================
                // FASE 7: PREPARACIÓN Y EJECUCIÓN DEL COMANDO SQL
                // ==============================================================================
                using var comando = new MySqlCommand(consultaSql, conexion);
                using var lector = await comando.ExecuteReaderAsync();

                // ==============================================================================
                // FASE 8: PROCESAMIENTO DE RESULTADOS
                // ==============================================================================
                while (await lector.ReadAsync())
                {
                    var fila = new Dictionary<string, object?>();
                    for (int indiceColumna = 0; indiceColumna < lector.FieldCount; indiceColumna++)
                    {
                        string nombreColumna = lector.GetName(indiceColumna);
                        object? valorColumna = lector.IsDBNull(indiceColumna) ? null : lector.GetValue(indiceColumna);

                        fila[nombreColumna] = valorColumna;
                    }
                    Resultados.Add(fila);
                }
            }
            catch (MySqlException sqlEx)
            {
                // =============================================================================
                // Manejo de errores especificos Maria DB / MySQL
                // =============================================================================
                throw ManejadorErroresMariaDB.Procesar(sqlEx, nombreTabla);
            }
            catch (Exception ex)
            {
                // Manejo genérico de errores
                throw new Exception("Error inesperado al obtener filas de la tabla.", ex);
            }

            // ==============================================================================
            // FASE 9: RETORNO DE RESULTADOS
            return Resultados;
            // ==============================================================================

        }


        public async Task<IReadOnlyList<Dictionary<string, object?>>> ObtenerPorClaveAsync(
                   string nombreTabla,     // Tabla objetivo para la consulta filtrada
                   string? esquema,        // Esquema de la tabla (opcional, puede ser null o vacío)
                   string nombreClave,     // Columna para aplicar filtro WHERE
                   string valor           // Valor específico a buscar (se usa como parámetro SQL)
               )
        {
            // ==============================================================================
            // FASE 1: VALIDACIONES DE ENTRADA
            // ==============================================================================
            if (string.IsNullOrWhiteSpace(nombreTabla))
                throw new ArgumentException("El nombre de la tabla no puede estar vacío.", nameof(nombreTabla));
            if (string.IsNullOrWhiteSpace(nombreClave))
                throw new ArgumentException("El nombre de la clave no puede estar vacío.", nameof(nombreClave));
            if (string.IsNullOrWhiteSpace(valor))
                throw new ArgumentException("El valor de la clave no puede estar vacío.", nameof(valor));

            // ==============================================================================
            // FASE 2: NORMALIZACIÓN DE PARÁMETROS
            // ==============================================================================

            // ==============================================================================
            // FASE 3: CONSTRUCCIÓN DE CONSULTA SQL ESPECÍFICA DE SQL SERVER
            // ==============================================================================
            string consultaSql = $@"
                SELECT *
                FROM `{nombreTabla}`
                WHERE `{nombreClave}` = @ValorClave
                LIMIT 100";

            // ==============================================================================
            // FASE 4: PREPARACIÓN DE ESTRUCTURAS DE DATOS
            // ==============================================================================

            var Resultados = new List<Dictionary<string, object?>>();

            try
            {
                // ==============================================================================
                // FASE 5: OBTENCIÓN DE CONEXIÓN (APLICANDO DIP)
                // ==============================================================================
                string cadenaConexion = _proveedorConexion.ObtenerCadenaConexion();

                // ==============================================================================
                // FASE 6: CONEXIÓN A SQL SERVER
                // ==============================================================================
                using var conexion = new MySqlConnection(cadenaConexion);
                await conexion.OpenAsync();
                // ==============================================================================
                // FASE 7: PREPARACIÓN Y EJECUCIÓN DEL COMANDO SQL
                // ==============================================================================

                using var comando = new MySqlCommand(consultaSql, conexion);
                comando.Parameters.AddWithValue("@ValorClave", valor); // Parámetro para evitar SQL Injection
                using var lector = await comando.ExecuteReaderAsync();

                // ==============================================================================
                // FASE 8: PROCESAMIENTO DE RESULTADOS
                // ==============================================================================
                while (await lector.ReadAsync())
                {
                    var fila = new Dictionary<string, object?>();
                    for (int indiceColumna = 0; indiceColumna < lector.FieldCount; indiceColumna++)
                    {
                        string nombreColumna = lector.GetName(indiceColumna);
                        object? valorColumna = lector.IsDBNull(indiceColumna) ? null : lector.GetValue(indiceColumna);

                        fila[nombreColumna] = valorColumna;
                    }
                    Resultados.Add(fila);

                }
            }
            catch (MySqlException sqlEx)
            {
                // =============================================================================
                // Manejo de errores especificos Maria DB / MySQL
                // =============================================================================
                throw ManejadorErroresMariaDB.Procesar(sqlEx, nombreTabla);
            }
            catch (Exception ex)
            {
                // Manejo genérico de errores
                throw new Exception("Error inesperado al obtener filas de la tabla por clave.", ex);
            }
            return Resultados;
            // ==============================================================================
        }

        public async Task<bool> CrearAsync(
            string nombreTabla,
            string? esquema,        // Esquema de la tabla (opcional, puede ser null o vacío)
            Dictionary<string, object?> datos,
            string? camposEncriptar = null
        )
        {
            //=============================================================================
            // FASE 1: VALIDACIONES DE ENTRADA
            //=============================================================================
            if (string.IsNullOrWhiteSpace(nombreTabla))
                throw new ArgumentException("El nombre de la tabla no puede estar vacío.", nameof(nombreTabla));
            if (datos == null || datos.Count == 0)
                throw new ArgumentException("Los datos no pueden estar vacíos.", nameof(datos));


            //=============================================================================
            // FASE 3: ENCRIPTACIÓN DE CAMPOS (SI APLICA)
            //=============================================================================

            var datosFinales = new Dictionary<string, object?>(datos);

            if (!string.IsNullOrWhiteSpace(camposEncriptar))
            {
                // Procesar lista de campos a encriptar separados por coma
                var camposAEncriptar = camposEncriptar.Split(',')
                    .Select(c => c.Trim())
                    .Where(c => !string.IsNullOrEmpty(c))
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);

                foreach (var campo in camposAEncriptar)
                {
                    if (datosFinales.ContainsKey(campo) && datosFinales[campo] != null)
                    {
                        string valorOriginal = datosFinales[campo]?.ToString() ?? "";

                        // Usar nuestra clase de utilidad BCrypt
                        datosFinales[campo] = webapicsharp.Servicios.Utilidades.EncriptacionBCrypt.Encriptar(valorOriginal);
                    }
                }
            }

            //=============================================================================
            // FASE 4: CONSTRUCCIÓN DE CONSULTA SQL ESPECÍFICA DE SQL SERVER
            //=============================================================================

            var columnas = string.Join(", ", datosFinales.Keys.Select(c => $"`{c}`"));
            var parametros = string.Join(", ", datosFinales.Keys.Select(c => $"@{c}"));
            string consultaSql = $@"
                INSERT INTO `{nombreTabla}` ({columnas})
                VALUES ({parametros})";

            try
            {
                //=============================================================================
                // FASE 5: OBTENCIÓN DE CONEXIÓN (APLICANDO DIP)
                //=============================================================================
                string cadenaConexion = _proveedorConexion.ObtenerCadenaConexion();

                using var conexion = new MySqlConnection(cadenaConexion);
                await conexion.OpenAsync();

                using var comando = new MySqlCommand(consultaSql, conexion);

                // Agregar todos los parámetros a la consulta SQL
                foreach (var kvp in datosFinales)
                {
                    // Convertir null de C# a null de mySQL
                    comando.Parameters.AddWithValue($"@{kvp.Key}", kvp.Value ?? DBNull.Value);
                }

                int filasAfectadas = await comando.ExecuteNonQueryAsync();

                return filasAfectadas > 0; // Retorna true si se insertó al menos una fila
            }
            catch (MySqlException sqlEx)
            {
                // Manejo específico de errores de mariaDB/MySQL
               throw ManejadorErroresMariaDB.Procesar(sqlEx, nombreTabla);
            }
            catch (Exception ex)
            {
                // Manejo genérico de errores
                throw new Exception("Error inesperado al insertar.", ex);
            }
        }

        public async Task<int> ActualizarAsync(
           string nombreTabla,
           string? esquema,        // Esquema de la tabla (opcional, puede ser null o vacío)
           string nombreClave,
           string valorClave,
           Dictionary<string, object?> datos,
           string? camposEncriptar = null
       )
        {
            //=============================================================================
            // FASE 1: VALIDACIONES DE ENTRADA
            //=============================================================================

            if (string.IsNullOrWhiteSpace(nombreTabla))
                throw new ArgumentException("El nombre de la tabla no puede estar vacío.", nameof(nombreTabla));
            if (string.IsNullOrWhiteSpace(nombreClave))
                throw new ArgumentException("El nombre de la clave no puede estar vacío.", nameof(nombreClave));
            if (string.IsNullOrWhiteSpace(valorClave))
                throw new ArgumentException("El valor de la clave no puede estar vacío.", nameof(valorClave));
            if (datos == null || datos.Count == 0)
                throw new ArgumentException("Los datos no pueden estar vacíos.", nameof(datos));

            //=============================================================================
            // FASE 2: ENCRIPTACIÓN DE CAMPOS (SI APLICA)
            //=============================================================================

            var datosFinales = new Dictionary<string, object?>(datos);

            if (!string.IsNullOrWhiteSpace(camposEncriptar))
            {
                // Procesar lista de campos a encriptar separados por coma
                var camposAEncriptar = camposEncriptar.Split(',')
                    .Select(c => c.Trim())
                    .Where(c => !string.IsNullOrEmpty(c))
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);

                foreach (var campo in camposAEncriptar)
                {
                    if (datosFinales.ContainsKey(campo) && datosFinales[campo] != null)
                    {
                        string valorOriginal = datosFinales[campo]?.ToString() ?? "";

                        // Usar nuestra clase de utilidad BCrypt
                        datosFinales[campo] = webapicsharp.Servicios.Utilidades.EncriptacionBCrypt.Encriptar(valorOriginal);
                    }
                }
            }

            //=============================================================================
            // FASE 3: CONSTRUCCIÓN DE CONSULTA SQL UPDATE ESPECÍFICA DE SQL SERVER
            //=============================================================================

            // Construir la clausula SET dinámicamente
            var clausulaSet = string.Join(", ", datosFinales.Keys.Select(c => $"`{c}` = @{c}"));

            //Construir la consulta SQL completa (UPDATE )
            string consultaSql = $@"
                UPDATE `{nombreTabla}`
                SET {clausulaSet}
                WHERE `{nombreClave}` = @ValorClave";

            try
            {
                //=============================================================================
                // FASE 4: OBTENCIÓN DE CONEXIÓN (APLICANDO DIP)
                //=============================================================================
                string cadenaConexion = _proveedorConexion.ObtenerCadenaConexion();

                using var conexion = new MySqlConnection(cadenaConexion);
                await conexion.OpenAsync();

                using var comando = new MySqlCommand(consultaSql, conexion);

                // Agregar todos los parámetros a la consulta SQL
                foreach (var kvp in datosFinales)
                {
                    // Convertir null de C# a DBNull.Value de SQL Server
                    comando.Parameters.AddWithValue($"@{kvp.Key}", kvp.Value ?? DBNull.Value);
                }

                // Parámetro para la cláusula WHERE
                comando.Parameters.AddWithValue("@ValorClave", valorClave);

                int filasAfectadas = await comando.ExecuteNonQueryAsync();

                return filasAfectadas; // Retorna el número de filas afectadas
            }
            catch (MySqlException sqlEx)
            {
                throw ManejadorErroresMariaDB.Procesar(sqlEx, nombreTabla);
                // Manejo específico de errores de MariaDB/MySQL
                
            }
            catch (Exception excepcionGeneral)
            {
                // Manejo genérico de errores 
                throw new InvalidOperationException($"Error inesperado al actualizar: {nombreTabla}' WHERE {nombreClave}='{valorClave}: {excepcionGeneral.Message}", excepcionGeneral);
            }

        }
        public async Task<int> EliminarAsync(
           string nombreTabla,
           string? esquema,        // Esquema de la tabla (opcional, puede ser null o vacío)
           string nombreClave,
           string valorClave
         )
        {
            //=============================================================================
            // FASE 1: VALIDACIONES DE ENTRADA
            //============================================================================= 

            if (string.IsNullOrWhiteSpace(nombreTabla))
                throw new ArgumentException("El nombre de la tabla no puede estar vacío.", nameof(nombreTabla));

            if (string.IsNullOrWhiteSpace(nombreClave))
                throw new ArgumentException("El nombre de la clave no puede estar vacío.", nameof(nombreClave));
            if (string.IsNullOrWhiteSpace(valorClave))

                throw new ArgumentException("El valor de la clave no puede estar vacío.", nameof(valorClave));

            //=============================================================================
            // FASE 3: CONSTRUCCIÓN DE CONSULTA SQL DELETE ESPECÍF
            //=============================================================================

            //Contruir la consulta SQL completa (DELETE) 
            string consultaSql = $@"
                DELETE FROM `{nombreTabla}`
                WHERE `{nombreClave}` = @ValorClave";

            try
            {
                //=============================================================================
                // FASE 4: OBTENCIÓN DE CONEXIÓN Y EJECUCIÓN SQL USANDO DIP
                //=============================================================================

                string cadenaConexion = _proveedorConexion.ObtenerCadenaConexion();

                using var conexion = new MySqlConnection(cadenaConexion);
                await conexion.OpenAsync();

                using var comando = new MySqlCommand(consultaSql, conexion);

                // Parámetro para la cláusula WHERE
                comando.Parameters.AddWithValue("@ValorClave", valorClave);

                int filasAfectadas = await comando.ExecuteNonQueryAsync();
                return filasAfectadas; // Retorna el número de filas afectadas

            }
            catch (MySqlException sqlEx)
            {
                // Manejo específico de errores de MariaDB/MySQL
                throw ManejadorErroresMariaDB.Procesar(sqlEx, nombreTabla);

            }
            catch (Exception excepcionGeneral)
            {
                throw new InvalidOperationException($"Error inesperado al eliminar: {nombreTabla}' WHERE {nombreClave}='{valorClave}: {excepcionGeneral.Message}", excepcionGeneral);
            }
        }

        public async Task<string?> ObtenerHashContrasenaAsync(
           string nombreTabla,
           string? esquema,        // Esquema de la tabla (opcional, puede ser null o vacío)
           string campoUsuario,
           string campoContrasena,
           string valorUsuario
       )
        {
            //=============================================================================
            // FASE 1: VALIDACIONES DE ENTRADA
            //=============================================================================

            if (string.IsNullOrWhiteSpace(nombreTabla))
                throw new ArgumentException("El nombre de la tabla no puede estar vacío.", nameof(nombreTabla));
            if (string.IsNullOrWhiteSpace(campoUsuario))
                throw new ArgumentException("El nombre del campo usuario no puede estar vacío.", nameof(campoUsuario));
            if (string.IsNullOrWhiteSpace(campoContrasena))
                throw new ArgumentException("El nombre del campo contraseña no puede estar vacío.", nameof(campoContrasena));
            if (string.IsNullOrWhiteSpace(valorUsuario))
                throw new ArgumentException("El valor del usuario no puede estar vacío.", nameof(valorUsuario));


            //=============================================================================
            // FASE 3: CONSTRUCCIÓN DE CONSULTA SQL SELECT ESPECÍFICA DE SQL SERVER
            //=============================================================================

            string consultaSql = $@"
                SELECT `{campoContrasena}`
                FROM `{nombreTabla}`
                WHERE `{campoUsuario}` = @ValorUsuario
                LIMIT 1";

            try
            {
                //=============================================================================
                // FASE 4: OBTENCIÓN DE CONEXIÓN (APLICANDO DIP)
                //=============================================================================
                string cadenaConexion = _proveedorConexion.ObtenerCadenaConexion();

                using var conexion = new MySqlConnection(cadenaConexion);
                await conexion.OpenAsync();

                using var comando = new MySqlCommand(consultaSql, conexion);
                comando.Parameters.AddWithValue("@ValorUsuario", valorUsuario); // Parámetro para evitar SQL Injection

                //Obtener resultado (hash de contraseña) como un solo valor

                var resultados = await comando.ExecuteScalarAsync();


                return resultados?.ToString(); // Retorna el hash de la contraseña convertido a cadena de texto o null si no se encontró
            }
            catch (MySqlException sqlEx)
            {
                // =============================================================================
                // Manejo de errores especificos Maria DB / MySQL
                // =============================================================================
               throw ManejadorErroresMariaDB.Procesar(sqlEx, nombreTabla);
            }
            catch (Exception ex)
            {
                // Manejo genérico de errores
                throw new Exception("Error inesperado al obtener hash de contraseña.", ex);
            }
        }

        //En caso de desocupe hacer mas cosas a partir de aca.



    }
}
