// RepositorioConsultasSqlServer.cs — Implementación específica para ejecutar consultas SQL parametrizadas en SQL Server
// Ubicación: Repositorios/RepositorioConsultasSqlServer.cs
//
// Principios SOLID aplicados:
// - SRP: Esta clase solo se encarga de ejecutar consultas SQL parametrizadas en SQL Server
// - DIP: Implementa IRepositorioConsultas e usa IProveedorConexion
// - OCP: Si se necesita PostgreSQL, se crea otra implementación sin tocar esta
// - LSP: Es intercambiable con cualquier otra implementación de IRepositorioConsultas

using System;                                          // Para excepciones y tipos básicos
using System.Collections.Generic;                      // Para List<>
using System.Threading.Tasks;                          // Para async/await
using System.Data;                                     // Para DataTable
using Microsoft.Data.SqlClient;                       // Para SqlConnection, SqlCommand, SqlParameter
using webapicsharp.Repositorios.Abstracciones;        // Para IRepositorioConsultas
using webapicsharp.Servicios.Abstracciones;           // Para IProveedorConexion

namespace webapicsharp.Repositorios
{
    /// <summary>
    /// Implementación específica para ejecutar consultas SQL parametrizadas en SQL Server.
    /// 
    /// Esta clase encapsula toda la lógica específica de SQL Server para consultas arbitrarias:
    /// - Conexión usando SqlConnection y SqlCommand
    /// - Manejo de parámetros SQL específicos de SQL Server
    /// - Conversión de tipos SQL Server a tipos .NET
    /// - Manejo de excepciones SqlException específicas
    /// - Optimizaciones de rendimiento para SQL Server
    /// 
    /// Diferencias con RepositorioLecturaSqlServer:
    /// - RepositorioLecturaSqlServer: SELECT * FROM tabla (consultas estándar)
    /// - RepositorioConsultasSqlServer: Consultas SQL arbitrarias (JOINs, agregaciones, etc.)
    /// 
    /// Reutiliza infraestructura existente pero especializada en consultas complejas.
    /// </summary>
    public class RepositorioConsultasSqlServer : IRepositorioConsultas
    {
        // Campo privado que mantiene referencia al proveedor de conexión
        // Aplica DIP: depende de abstracción, no de implementación concreta
        private readonly IProveedorConexion _proveedorConexion;

        /// <summary>
        /// Constructor que recibe el proveedor de conexión mediante inyección de dependencias.
        /// 
        /// Reutiliza la misma infraestructura que el resto de la aplicación:
        /// - IProveedorConexion para obtener cadenas de conexión
        /// - Configuración centralizada en Program.cs
        /// - Consistencia con otros repositorios de la aplicación
        /// 
        /// Aplica DIP: no sabe cómo se obtienen las cadenas de conexión,
        /// solo sabe que puede pedírselas a IProveedorConexion.
        /// </summary>
        /// <param name="proveedorConexion">Proveedor inyectado automáticamente</param>
        public RepositorioConsultasSqlServer(IProveedorConexion proveedorConexion)
        {
            _proveedorConexion = proveedorConexion ?? throw new ArgumentNullException(
                nameof(proveedorConexion),
                "IProveedorConexion no puede ser null. Verificar registro en Program.cs."
            );
        }

        /// <summary>
        /// Implementa IRepositorioConsultas.EjecutarConsultaParametrizadaAsync para SQL Server.
        /// 
        /// Proceso específico para SQL Server:
        /// 1. Validar parámetros de entrada
        /// 2. Obtener cadena de conexión via IProveedorConexion
        /// 3. Crear conexión SqlConnection específica de SQL Server
        /// 4. Preparar SqlCommand con la consulta y parámetros
        /// 5. Ejecutar consulta de forma asíncrona
        /// 6. Cargar resultados en DataTable manteniendo esquema
        /// 7. Manejar errores específicos de SQL Server
        /// 8. Liberar recursos automáticamente con using
        /// </summary>
        /// <param name="consultaSQL">Consulta SQL a ejecutar</param>
        /// <param name="parametros">Parámetros SQL para la consulta</param>
        /// <returns>DataTable con resultados</returns>
        public async Task<DataTable> EjecutarConsultaParametrizadaAsync(
            string consultaSQL,
            List<SqlParameter>? parametros
        )
        {
            // FASE 1: VALIDACIONES DE ENTRADA
            if (string.IsNullOrWhiteSpace(consultaSQL))
                throw new ArgumentException(
                    "La consulta SQL no puede estar vacía.",
                    nameof(consultaSQL)
                );

            // FASE 2: PREPARACIÓN DE ESTRUCTURA DE DATOS
            var dataTable = new DataTable();

            try
            {
                // FASE 3: OBTENCIÓN DE CONEXIÓN (APLICANDO DIP)
                // Usa la infraestructura existente sin duplicar configuración
                string cadenaConexion = _proveedorConexion.ObtenerCadenaConexion();

                // FASE 4: CONEXIÓN A SQL SERVER
                // Usar 'using' garantiza liberación de recursos incluso si hay excepción
                using var conexion = new SqlConnection(cadenaConexion);
                await conexion.OpenAsync();

                // FASE 5: PREPARACIÓN DEL COMANDO SQL
                using var comando = new SqlCommand(consultaSQL, conexion);

                // Configurar timeout para consultas complejas
                comando.CommandTimeout = 30; // 30 segundos, ajustable según necesidades

                // FASE 6: AGREGAR PARÁMETROS DE FORMA SEGURA
                if (parametros != null && parametros.Count > 0)
                {
                    foreach (var parametro in parametros)
                    {
                        // Validar nombre de parámetro
                        if (string.IsNullOrWhiteSpace(parametro.ParameterName))
                            throw new ArgumentException("Parámetro con nombre vacío encontrado");

                        // Clonar parámetro para evitar problemas de reutilización
                        var parametroClonado = new SqlParameter
                        {
                            ParameterName = parametro.ParameterName,
                            Value = parametro.Value ?? DBNull.Value,
                            SqlDbType = parametro.SqlDbType,
                            Size = parametro.Size,
                            Precision = parametro.Precision,
                            Scale = parametro.Scale
                        };

                        comando.Parameters.Add(parametroClonado);
                    }
                }

                // FASE 7: EJECUCIÓN DE LA CONSULTA
                using var lector = await comando.ExecuteReaderAsync();

                // Load() carga automáticamente el esquema y los datos
                // Mantiene información de tipos de columna, nombres, etc.
                dataTable.Load(lector);

                return dataTable;
            }
            catch (SqlException sqlEx)
            {
                // FASE 8: MANEJO ESPECÍFICO DE ERRORES SQL SERVER
                // Traducir códigos de error SQL específicos a mensajes claros
                string mensajeError = sqlEx.Number switch
                {
                    2 => "Timeout: La consulta tardó demasiado en ejecutarse",
                    207 => "Nombre de columna inválido en la consulta SQL",
                    208 => "Tabla o vista especificada no existe en la base de datos",
                    102 => "Error de sintaxis en la consulta SQL",
                    515 => "Valor null no permitido en columna que no acepta nulls",
                    547 => "Violación de restricción de clave foránea",
                    2812 => "Procedimiento almacenado no encontrado",
                    _ => $"Error SQL Server (Código {sqlEx.Number}): {sqlEx.Message}"
                };

                throw new InvalidOperationException(
                    $"Error al ejecutar consulta SQL: {mensajeError}",
                    sqlEx
                );
            }
            catch (InvalidOperationException)
            {
                // Re-lanzar excepciones InvalidOperation sin modificar
                // (pueden venir del proveedor de conexión)
                throw;
            }
            catch (Exception ex)
            {
                // FASE 9: MANEJO DE ERRORES INESPERADOS
                throw new InvalidOperationException(
                    $"Error inesperado al ejecutar consulta: {ex.Message}",
                    ex
                );
            }
        }

        /// <summary>
        /// Implementa IRepositorioConsultas.ValidarConsultaAsync para SQL Server.
        /// 
        /// Usa SET PARSEONLY ON de SQL Server para validar sintaxis sin ejecutar.
        /// Útil para verificar consultas antes de ejecutarlas en producción.
        /// </summary>
        /// <param name="consultaSQL">Consulta a validar</param>
        /// <param name="parametros">Parámetros a validar</param>
        /// <returns>Tupla con resultado de validación</returns>
        public async Task<(bool esValida, string? mensajeError)> ValidarConsultaAsync(
            string consultaSQL,
            List<SqlParameter>? parametros
        )
        {
            if (string.IsNullOrWhiteSpace(consultaSQL))
                return (false, "La consulta no puede estar vacía");

            try
            {
                string cadenaConexion = _proveedorConexion.ObtenerCadenaConexion();

                using var conexion = new SqlConnection(cadenaConexion);
                await conexion.OpenAsync();

                // FASE 1: VALIDAR SINTAXIS CON SET PARSEONLY ON
                using var comandoParseOnly = new SqlCommand("SET PARSEONLY ON", conexion);
                await comandoParseOnly.ExecuteNonQueryAsync();

                // FASE 2: INTENTAR PARSEAR LA CONSULTA
                using var comandoValidacion = new SqlCommand(consultaSQL, conexion);
                comandoValidacion.CommandTimeout = 5; // Timeout corto para validación

                // Agregar parámetros para validación completa
                if (parametros != null)
                {
                    foreach (var parametro in parametros)
                    {
                        comandoValidacion.Parameters.Add(new SqlParameter
                        {
                            ParameterName = parametro.ParameterName,
                            SqlDbType = parametro.SqlDbType,
                            Value = DBNull.Value // Solo para validación de estructura
                        });
                    }
                }

                await comandoValidacion.ExecuteNonQueryAsync();

                // FASE 3: RESTAURAR PARSEONLY OFF
                using var comandoParseOff = new SqlCommand("SET PARSEONLY OFF", conexion);
                await comandoParseOff.ExecuteNonQueryAsync();

                return (true, null);
            }
            catch (SqlException sqlEx)
            {
                string mensajeError = sqlEx.Number switch
                {
                    102 => "Error de sintaxis SQL",
                    207 => "Nombre de columna inválido",
                    208 => "Objeto no válido (tabla/vista no existe)",
                    _ => $"Error de validación: {sqlEx.Message}"
                };

                return (false, mensajeError);
            }
            catch (Exception ex)
            {
                return (false, $"Error inesperado en validación: {ex.Message}");
            }
        }
        /// <summary>
        /// Implementa IRepositorioConsultas.EjecutarProcedimientoAlmacenadoAsync para SQL Server.
        /// 
        /// Ejecuta un procedimiento almacenado con parámetros de forma asíncrona.
        /// Similar a EjecutarConsultaParametrizadaAsync pero usando CommandType.StoredProcedure.
        /// </summary>
        /// <param name="nombreSP">Nombre del procedimiento almacenado a ejecutar</param>
        /// <param name="parametros">Lista de parámetros SQL para el procedimiento</param>
        /// <returns>DataTable con los resultados del procedimiento almacenado</returns>
        public async Task<DataTable> EjecutarProcedimientoAlmacenadoAsync(
            string nombreSP,
            List<SqlParameter>? parametros)
        {
            // FASE 1: VALIDACIONES DE ENTRADA
            if (string.IsNullOrWhiteSpace(nombreSP))
                throw new ArgumentException(
                    "El nombre del procedimiento almacenado no puede estar vacío.",
                    nameof(nombreSP)
                );

            // FASE 2: PREPARACIÓN DE ESTRUCTURA DE DATOS
            var dataTable = new DataTable();

            try
            {
                // FASE 3: OBTENCIÓN DE CONEXIÓN (APLICANDO DIP)
                string cadenaConexion = _proveedorConexion.ObtenerCadenaConexion();

                // FASE 4: CONEXIÓN A SQL SERVER
                using var conexion = new SqlConnection(cadenaConexion);
                await conexion.OpenAsync();

                // FASE 5: PREPARACIÓN DEL COMANDO PARA PROCEDIMIENTO ALMACENADO
                using var comando = new SqlCommand(nombreSP, conexion);
                comando.CommandType = CommandType.StoredProcedure;  // DIFERENCIA CLAVE: Tipo StoredProcedure
                comando.CommandTimeout = 30;

                // FASE 6: AGREGAR PARÁMETROS DE FORMA SEGURA
                if (parametros != null && parametros.Count > 0)
                {
                    foreach (var parametro in parametros)
                    {
                        if (string.IsNullOrWhiteSpace(parametro.ParameterName))
                            throw new ArgumentException("Parámetro con nombre vacío encontrado");

                        // Clonar parámetro para evitar problemas de reutilización
                        var parametroClonado = new SqlParameter
                        {
                            ParameterName = parametro.ParameterName,
                            Value = parametro.Value ?? DBNull.Value,
                            SqlDbType = parametro.SqlDbType,
                            Size = parametro.Size,
                            Precision = parametro.Precision,
                            Scale = parametro.Scale
                        };

                        comando.Parameters.Add(parametroClonado);
                    }
                }

                // FASE 7: EJECUCIÓN DEL PROCEDIMIENTO ALMACENADO
                using var lector = await comando.ExecuteReaderAsync();
                dataTable.Load(lector);

                return dataTable;
            }
            catch (SqlException sqlEx)
            {
                // FASE 8: MANEJO ESPECÍFICO DE ERRORES SQL SERVER PARA SP
                string mensajeError = sqlEx.Number switch
                {
                    2812 => "Procedimiento almacenado no encontrado",
                    201 => "Error en parámetros del procedimiento almacenado",
                    2 => "Timeout: El procedimiento tardó demasiado en ejecutarse",
                    _ => $"Error SQL Server en procedimiento almacenado (Código {sqlEx.Number}): {sqlEx.Message}"
                };

                throw new InvalidOperationException(
                    $"Error al ejecutar procedimiento almacenado '{nombreSP}': {mensajeError}",
                    sqlEx
                );
            }
            catch (Exception ex)
            {
                // FASE 9: MANEJO DE ERRORES INESPERADOS
                throw new InvalidOperationException(
                    $"Error inesperado al ejecutar procedimiento almacenado '{nombreSP}': {ex.Message}",
                    ex
                );
            }
        }

        /// <summary>
        /// Implementa IRepositorioConsultas.ObtenerEsquemaTablaAsync para SQL Server.
        /// 
        /// Busca en qué esquema existe una tabla específica, priorizando el esquema sugerido.
        /// Utiliza la consulta SQL original probada que funciona correctamente.
        /// </summary>
        /// <param name="nombreTabla">Nombre de la tabla a buscar</param>
        /// <param name="esquemaPredeterminado">Esquema donde buscar primero</param>
        /// <returns>Nombre del esquema donde existe la tabla, o null si no existe</returns>
        /// <exception cref="ArgumentException">Se lanza si nombreTabla es null o vacío</exception>
        /// <exception cref="InvalidOperationException">Se lanza si hay errores en la consulta SQL</exception>
        public async Task<string?> ObtenerEsquemaTablaAsync(string nombreTabla, string esquemaPredeterminado)
        {
            if (string.IsNullOrWhiteSpace(nombreTabla))
                throw new ArgumentException("El nombre de la tabla no puede estar vacío.", nameof(nombreTabla));

            try
            {
                string cadenaConexion = _proveedorConexion.ObtenerCadenaConexion();

                using var conexion = new SqlConnection(cadenaConexion);
                await conexion.OpenAsync();

                // Consulta SQL original probada y funcional
                string consultaSql = @"
                    SELECT TOP 1 TABLE_SCHEMA 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_NAME = @nombreTabla 
                    ORDER BY 
                        CASE WHEN TABLE_SCHEMA = @esquema THEN 0 ELSE 1 END, 
                        TABLE_SCHEMA";

                using var comando = new SqlCommand(consultaSql, conexion);
                comando.Parameters.Add(new SqlParameter("@nombreTabla", nombreTabla));
                comando.Parameters.Add(new SqlParameter("@esquema", esquemaPredeterminado));

                var resultado = await comando.ExecuteScalarAsync();
                return resultado?.ToString();
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Error al buscar esquema para tabla '{nombreTabla}': {ex.Message}",
                    ex
                );
            }
        }

        /// <summary>
        /// Implementa IRepositorioConsultas.ObtenerEstructuraTablaAsync para SQL Server.
        /// 
        /// Obtiene información detallada de columnas, tipos, restricciones de una tabla específica.
        /// Incluye datos sobre claves primarias, tipos de datos, longitudes y propiedades de identidad.
        /// Utiliza la consulta SQL original probada que funciona correctamente.
        /// </summary>
        /// <param name="nombreTabla">Nombre de la tabla</param>
        /// <param name="esquema">Esquema de la tabla</param>
        /// <returns>DataTable con la estructura completa de la tabla</returns>
        /// <exception cref="ArgumentException">Se lanza si nombreTabla es null o vacío</exception>
        /// <exception cref="InvalidOperationException">Se lanza si hay errores en la consulta SQL</exception>
        public async Task<DataTable> ObtenerEstructuraTablaAsync(string nombreTabla, string esquema)
        {
            if (string.IsNullOrWhiteSpace(nombreTabla))
                throw new ArgumentException("El nombre de la tabla no puede estar vacío.", nameof(nombreTabla));

            var dataTable = new DataTable();

            try
            {
                string cadenaConexion = _proveedorConexion.ObtenerCadenaConexion();

                using var conexion = new SqlConnection(cadenaConexion);
                await conexion.OpenAsync();

                // Consulta SQL original probada y funcional
                string consultaSql = @"
                    SELECT c.COLUMN_NAME AS Nombre, c.DATA_TYPE AS TipoSql, c.CHARACTER_MAXIMUM_LENGTH AS Longitud,
                        c.IS_NULLABLE AS Nullable, c.COLUMN_DEFAULT AS ValorDefecto,
                        COLUMNPROPERTY(OBJECT_ID(QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME)), c.COLUMN_NAME, 'IsIdentity') AS EsIdentidad,
                        CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS EsPrimaria
                    FROM INFORMATION_SCHEMA.COLUMNS c
                    LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE pk
                        ON pk.TABLE_SCHEMA = c.TABLE_SCHEMA AND pk.TABLE_NAME = c.TABLE_NAME
                        AND pk.COLUMN_NAME = c.COLUMN_NAME
                        AND OBJECTPROPERTY(OBJECT_ID(pk.CONSTRAINT_NAME), 'IsPrimaryKey') = 1
                    WHERE c.TABLE_NAME = @nombreTabla AND c.TABLE_SCHEMA = @esquema
                    ORDER BY c.ORDINAL_POSITION";

                using var comando = new SqlCommand(consultaSql, conexion);
                comando.Parameters.Add(new SqlParameter("@nombreTabla", nombreTabla));
                comando.Parameters.Add(new SqlParameter("@esquema", esquema));

                using var lector = await comando.ExecuteReaderAsync();
                dataTable.Load(lector);

                return dataTable;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Error al obtener estructura de tabla '{esquema}.{nombreTabla}': {ex.Message}",
                    ex
                );
            }
        }

        /// <summary>
        /// Implementa IRepositorioConsultas.ObtenerEstructuraBaseDatosAsync para SQL Server.
        /// 
        /// Obtiene estructura completa de la base de datos con todas las tablas, columnas y sus propiedades.
        /// Incluye información de esquemas, tipos de datos, longitudes, propiedades de identidad y posiciones.
        /// Utiliza la consulta SQL original probada que funciona correctamente.
        /// </summary>
        /// <param name="nombreBD">Nombre de la base de datos (opcional, si es null usa la BD actual)</param>
        /// <returns>DataTable con la estructura jerárquica de toda la base de datos</returns>
        /// <exception cref="InvalidOperationException">Se lanza si hay errores en la consulta SQL</exception>
        public async Task<DataTable> ObtenerEstructuraBaseDatosAsync(string? nombreBD)
        {
            var dataTable = new DataTable();

            try
            {
                string cadenaConexion = _proveedorConexion.ObtenerCadenaConexion();

                using var conexion = new SqlConnection(cadenaConexion);
                await conexion.OpenAsync();

                // Consulta SQL original probada y funcional
                string consultaSql = @"
                    SELECT 
                        t.TABLE_SCHEMA AS Esquema,
                        t.TABLE_NAME AS Tabla,
                        c.COLUMN_NAME AS Columna,
                        c.DATA_TYPE AS TipoDato,
                        c.CHARACTER_MAXIMUM_LENGTH AS LongitudMaxima,
                        c.IS_NULLABLE AS Nullable,
                        CASE WHEN COLUMNPROPERTY(OBJECT_ID(t.TABLE_SCHEMA + '.' + t.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') = 1 THEN 'SI' ELSE 'NO' END AS Identidad,
                        c.ORDINAL_POSITION AS Posicion
                    FROM INFORMATION_SCHEMA.TABLES t
                    INNER JOIN INFORMATION_SCHEMA.COLUMNS c
                        ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME
                    WHERE t.TABLE_TYPE = 'BASE TABLE'
                    ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME, c.ORDINAL_POSITION";

                using var comando = new SqlCommand(consultaSql, conexion);

                using var lector = await comando.ExecuteReaderAsync();
                dataTable.Load(lector);

                return dataTable;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Error al obtener estructura de base de datos: {ex.Message}",
                    ex
                );
            }
        }


        //aquí podríamos agregar más métodos específicos si fuera necesario
    }
}

// NOTAS PEDAGÓGICAS para el tutorial:
//
// 1. ESTA ES LA IMPLEMENTACIÓN CONCRETA ESPECÍFICA DE SQL SERVER:
//    - Implementa IRepositorioConsultas definida en Abstracciones/
//    - Define CÓMO ejecutar consultas SQL específicamente en SQL Server
//    - Usa SqlConnection, SqlCommand, SqlParameter específicos de SQL Server
//    - Maneja SqlException con códigos específicos de SQL Server
//
// 2. REUTILIZACIÓN DE INFRAESTRUCTURA EXISTENTE:
//    - Usa IProveedorConexion ya configurado en la aplicación
//    - Mantiene consistencia con RepositorioLecturaSqlServer
//    - Se integra naturalmente con el sistema de inyección de dependencias
//    - Aprovecha configuración existente sin duplicarla
//
// 3. ESPECIALIZACIÓN EN CONSULTAS COMPLEJAS:
//    - Manejo robusto de parámetros SQL (clonación para evitar concurrencia)
//    - Timeout configurable para consultas de larga duración
//    - Validación de sintaxis usando SET PARSEONLY ON de SQL Server
//    - Mapeo detallado de errores SQL específicos
//
// 4. APLICACIÓN PRÁCTICA DE DIP:
//    - Depende de IProveedorConexion (abstracción), no de implementación
//    - Facilita testing: IProveedorConexion se puede mockear
//    - Permite cambios en configuración sin modificar este código
//    - Mantiene separación clara de responsabilidades
//
// 5. MANEJO DE ERRORES ESPECÍFICO DE SQL SERVER:
//    - Traducción de códigos numéricos SQL a mensajes claros
//    - Categorización de errores (sintaxis, permisos, conectividad)
//    - Preservación de información original como InnerException
//    - Mensajes orientados a facilitar debugging
//
// 6. OPTIMIZACIONES DE RENDIMIENTO:
//    - Uso de async/await para no bloquear threads
//    - Using statements para liberación automática de recursos
//    - Clonación de parámetros para evitar problemas de concurrencia
//    - DataTable.Load() eficiente para cargar datos con esquema
//
// 7. ¿QUÉ VIENE DESPUÉS EN EL TUTORIAL?
//    - Registrar en Program.cs: AddScoped<IRepositorioConsultas, RepositorioConsultasSqlServer>
//    - Modificar ServicioConsultas para usar IRepositorioConsultas en lugar de conexión directa
//    - Crear ConsultasController que use ServicioConsultas
//
// 8. EXTENSIBILIDAD FUTURA:
//    - Se puede crear RepositorioConsultasPostgreSQL siguiendo el mismo patrón
//    - Agregar más métodos especializados (batch queries, streaming)
//    - Implementar métricas de rendimiento de consultas
//    - Integrar con sistemas de auditoría y monitoreo
//
// 9. TESTING DE ESTA CLASE:
//    - IProveedorConexion se mockea fácilmente
//    - Consultas se pueden probar con base de datos en memoria
//    - Validaciones se pueden verificar independientemente
//    - Manejo de errores se prueba con conexiones que fallan
//
// 10. CONSIDERACIONES DE SEGURIDAD:
//     - Parámetros SQL previenen inyección automáticamente
//     - Validación de sintaxis sin ejecución (SET PARSEONLY ON)
//     - Timeouts configurables para evitar consultas infinitas
//     - Manejo seguro de recursos con using statements