// RepositorioLecturaSqlServer.cs — Implementación concreta para leer datos usando ADO.NET y SQL Server
// Ubicación: Repositorios/RepositorioLecturaSqlServer.cs
//
// Principios SOLID aplicados:
// - SRP: Esta clase solo se encarga de leer datos desde SQL Server, nada más
// - DIP: Implementa IRepositorioLecturaTabla (abstracción) y usa IProveedorConexion
// - OCP: Si mañana se necesita PostgreSQL, se crea otra implementación sin tocar esta
// - LSP: Es completamente intercambiable con cualquier otra implementación de IRepositorioLecturaTabla

using System;                                          // Para excepciones y tipos básicos del sistema
using System.Collections.Generic;                      // Para List<> y Dictionary<> genéricos
using System.Threading.Tasks;                          // Para programación asíncrona con async/await
using Microsoft.Data.SqlClient;                       // Para conectar y ejecutar comandos en SQL Server
using webapicsharp.Repositorios.Abstracciones;        // Para implementar la interfaz IRepositorioLecturaTabla
using webapicsharp.Servicios.Abstracciones;           // Para usar IProveedorConexion y obtener cadenas de conexión
using webapicsharp.Servicios.Utilidades;

namespace webapicsharp.Repositorios
{
    /// <summary>
    /// Implementación específica para leer datos de SQL Server usando ADO.NET.
    /// 
    /// Esta clase encapsula toda la lógica específica de SQL Server:
    /// - Sintaxis SQL específica (TOP en lugar de LIMIT, corchetes para escapar nombres)
    /// - Tipos de datos específicos de SQL Server y su conversión a tipos .NET
    /// - Manejo de conexiones SqlConnection y SqlCommand
    /// - Esquema por defecto "dbo" característico de SQL Server
    /// - Manejo específico de excepciones SqlException
    /// 
    /// ¿Por qué una clase específica para SQL Server?
    /// Cada proveedor de base de datos tiene particularidades:
    /// - SQL Server: TOP, corchetes [], esquema "dbo", SqlConnection
    /// - PostgreSQL: LIMIT, comillas "", esquema "public", NpgsqlConnection
    /// - MariaDB: LIMIT, backticks ``, sin esquemas, MySqlConnection
    /// 
    /// Cumple estrictamente SRP: solo acceso a datos de SQL Server.
    /// No contiene validaciones de negocio ni lógica de presentación.
    /// </summary>
    public class RepositorioLecturaSqlServer : IRepositorioLecturaTabla
    {
        // Campo privado que mantiene la referencia al proveedor de conexión inyectado
        // Aplica DIP: depende de abstracción (IProveedorConexion), no de implementación concreta
        // Este campo se inicializa una vez en el constructor y se usa en todos los métodos
        private readonly IProveedorConexion _proveedorConexion;

        /// <summary>
        /// Constructor que recibe el proveedor de conexión mediante inyección de dependencias.
        /// 
        /// El flujo completo de inyección de dependencias es:
        /// 1. Program.cs registra: AddScoped<IRepositorioLecturaTabla, RepositorioLecturaSqlServer>()
        /// 2. Program.cs registra: AddSingleton<IProveedorConexion, ProveedorConexion>()
        /// 3. Cuando un servicio solicita IRepositorioLecturaTabla, el contenedor DI:
        ///    - Crea nueva instancia de RepositorioLecturaSqlServer (porque es Scoped)
        ///    - Ve que el constructor necesita IProveedorConexion
        ///    - Busca en su registro y encuentra ProveedorConexion
        ///    - Inyecta automáticamente la instancia de ProveedorConexion (reutilizada porque es Singleton)
        ///    - Llama a este constructor con las dependencias resueltas
        /// 
        /// Beneficios de este patrón:
        /// - Aplica DIP: esta clase no sabe cómo se obtienen las cadenas de conexión
        /// - Solo sabe que puede pedírselas a IProveedorConexion (abstracción)
        /// - Facilita testing: se puede inyectar un IProveedorConexion mock
        /// - Facilita cambios: se puede cambiar ProveedorConexion sin tocar este código
        /// - Centraliza configuración: todo se maneja en Program.cs
        /// </summary>
        /// <param name="proveedorConexion">
        /// Proveedor que entrega cadenas de conexión según configuración.
        /// Se inyecta automáticamente desde el contenedor de servicios de ASP.NET Core.
        /// No puede ser null porque el contenedor DI garantiza su existencia.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Se lanza si proveedorConexion es null, lo cual indicaría un problema grave
        /// en la configuración de inyección de dependencias en Program.cs.
        /// En funcionamiento normal, esta excepción nunca debería lanzarse.
        /// </exception>
        public RepositorioLecturaSqlServer(IProveedorConexion proveedorConexion)
        {
            // Validación defensiva: asegurar que la inyección de dependencias funcionó correctamente
            // Esta validación protege contra errores de configuración en Program.cs
            // Si proveedorConexion es null, significa que el registro de servicios está mal configurado
            _proveedorConexion = proveedorConexion ?? throw new ArgumentNullException(
                nameof(proveedorConexion),
                "IProveedorConexion no puede ser null. Verificar registro de servicios en Program.cs."
            );
        }

        /// <summary>
        /// Implementa IRepositorioLecturaTabla.ObtenerFilasAsync para SQL Server.
        /// 
        /// Este método es el corazón de la clase: toma parámetros genéricos y los convierte
        /// en una consulta SQL específica de SQL Server, ejecuta la consulta, y devuelve
        /// los resultados en el formato estándar Dictionary<string, object?>.
        /// 
        /// Proceso específico para SQL Server:
        /// 1. Validar parámetros de entrada según reglas de negocio
        /// 2. Aplicar valores por defecto específicos de SQL Server (esquema="dbo", limite=1000)
        /// 3. Construir consulta SQL con sintaxis específica de SQL Server (TOP, corchetes)
        /// 4. Obtener cadena de conexión vía IProveedorConexion (aplicando DIP)
        /// 5. Crear y ejecutar SqlConnection/SqlCommand de forma asíncrona
        /// 6. Procesar SqlDataReader y mapear resultados a Dictionary<string, object?>
        /// 7. Manejar errores específicos de SQL Server con contexto apropiado
        /// 8. Garantizar liberación de recursos con using statements
        /// 
        /// Optimizaciones y características específicas aplicadas:
        /// - Uso de TOP en la consulta SQL (más eficiente que LIMIT + OFFSET)
        /// - Corchetes [] para escapar nombres de esquema/tabla (evita palabras reservadas)
        /// - Conversión explícita de DBNull.Value a null C# (compatibilidad de tipos)
        /// - Liberación automática de recursos con using (evita memory leaks)
        /// - Programación asíncrona para no bloquear threads del pool
        /// </summary>
        /// <param name="nombreTabla">
        /// Nombre de la tabla a consultar en SQL Server. Obligatorio y no puede estar vacío.
        /// Se escapa automáticamente con corchetes para evitar problemas con palabras reservadas.
        /// Ejemplos válidos: "usuarios", "productos", "pedidos", "order" (palabra reservada pero funciona con [])
        /// </param>
        /// <param name="esquema">
        /// Esquema de la tabla en SQL Server. Si es null o vacío, usa "dbo" por defecto.
        /// Se escapa automáticamente con corchetes.
        /// Ejemplos: "dbo" (por defecto), "ventas", "inventario", "rrhh"
        /// </param>
        /// <param name="limite">
        /// Número máximo de filas a retornar. Si es null, usa 1000 por defecto para evitar consultas masivas.
        /// Se implementa con TOP en la consulta SQL para máxima eficiencia.
        /// Ejemplos: null (usa 1000), 50, 500, 10000
        /// </param>
        /// <returns>
        /// Lista inmutable de diccionarios donde cada diccionario representa una fila:
        /// - Clave: nombre de columna tal como está definido en SQL Server
        /// - Valor: dato de la columna, con DBNull.Value convertido a null C#
        /// 
        /// Conversión de tipos SQL Server → .NET:
        /// - INT → int, BIGINT → long, DECIMAL → decimal, FLOAT → double
        /// - VARCHAR/NVARCHAR → string, CHAR → string
        /// - DATETIME/DATETIME2 → DateTime, DATE → DateOnly (si disponible)
        /// - BIT → bool, BINARY/VARBINARY → byte[]
        /// - NULL (DBNull.Value) → null
        /// 
        /// Si no hay registros, devuelve lista vacía (Count = 0), NUNCA null.
        /// </returns>
        /// <exception cref="ArgumentException">
        /// Se lanza cuando nombreTabla es null, vacío o solo espacios en blanco.
        /// Esta validación evita construir consultas SQL inválidas.
        /// </exception>
        /// <exception cref="InvalidOperationException">
        /// Se lanza cuando hay problemas operacionales que no son culpa del desarrollador:
        /// - La tabla especificada no existe en el esquema especificado
        /// - No se puede conectar a SQL Server (servidor caído, red, etc.)
        /// - Hay errores en la ejecución de la consulta SQL
        /// - Problemas de permisos de acceso a la tabla
        /// - Timeout en la consulta (tabla muy grande o servidor lento)
        /// 
        /// La excepción incluye contexto específico para facilitar debugging.
        /// </exception>
        public async Task<IReadOnlyList<Dictionary<string, object?>>> ObtenerFilasAsync(
            string nombreTabla,    // Nombre de la tabla (requerido)
            string? esquema,       // Esquema de SQL Server (opcional, por defecto "dbo")
            int? limite           // Límite de registros (opcional, por defecto 1000)
        )
        {
            // ==============================================================================
            // FASE 1: VALIDACIONES DE ENTRADA
            // ==============================================================================

            // Verificar que el nombre de tabla sea válido antes de construir SQL
            // Esta validación temprana evita errores más crípticos en la base de datos
            if (string.IsNullOrWhiteSpace(nombreTabla))
                throw new ArgumentException(
                    "El nombre de la tabla no puede estar vacío.",
                    nameof(nombreTabla)
                );

            // ==============================================================================
            // FASE 2: NORMALIZACIÓN DE PARÁMETROS
            // ==============================================================================

            // Aplicar valores por defecto específicos de SQL Server
            // "dbo" es el esquema por defecto en SQL Server (Database Owner)
            string esquemaFinal = string.IsNullOrWhiteSpace(esquema) ? "dbo" : esquema.Trim();

            // 1000 es un límite razonable para evitar consultas masivas que puedan
            // consumir mucha memoria o tiempo, pero suficiente para la mayoría de casos de uso
            int limiteFinal = limite ?? 1000;

            // ==============================================================================
            // FASE 3: CONSTRUCCIÓN DE CONSULTA SQL ESPECÍFICA DE SQL SERVER
            // ==============================================================================

            // Construir consulta SQL con sintaxis específica de SQL Server:
            // - TOP (limiteFinal) en lugar de LIMIT (usado por PostgreSQL/MySQL)
            // - Corchetes [] para escapar nombres de esquema/tabla (protege contra palabras reservadas)
            // - Formato [esquema].[tabla] característico de SQL Server
            string consultaSql = $"SELECT TOP ({limiteFinal}) * FROM [{esquemaFinal}].[{nombreTabla}]";

            // ¿Por qué usar corchetes []?
            // - Protegen contra palabras reservadas: [order], [user], [select], etc.
            // - Permiten nombres con espacios: [Nombre de Tabla]
            // - Son el estándar de SQL Server (PostgreSQL usa "", MySQL usa ``)

            // ==============================================================================
            // FASE 4: PREPARACIÓN DE ESTRUCTURAS DE DATOS
            // ==============================================================================

            // Crear lista mutable para recolectar resultados durante el procesamiento
            // Se convertirá a IReadOnlyList antes de devolverla al cliente
            var resultados = new List<Dictionary<string, object?>>();

            try
            {
                // ==============================================================================
                // FASE 5: OBTENCIÓN DE CONEXIÓN (APLICANDO DIP)
                // ==============================================================================

                // No sabemos de dónde viene la cadena (appsettings.json, variables de entorno, etc.)
                // Solo sabemos que IProveedorConexion nos la puede proporcionar
                // Esto aplica DIP: dependemos de abstracción, no de implementación concreta
                string cadenaConexion = _proveedorConexion.ObtenerCadenaConexion();

                // ==============================================================================
                // FASE 6: CONEXIÓN A SQL SERVER
                // ==============================================================================

                // Crear conexión específica de SQL Server usando Microsoft.Data.SqlClient
                // Usar 'using' para garantizar liberación de recursos automáticamente
                // El 'using' llama a Dispose() automáticamente, incluso si ocurre una excepción
                using var conexion = new SqlConnection(cadenaConexion);

                // Abrir conexión de forma asíncrona para no bloquear el thread actual
                // Esto permite que el servidor web atienda otras peticiones mientras esperamos la conexión
                await conexion.OpenAsync();

                // ==============================================================================
                // FASE 7: PREPARACIÓN Y EJECUCIÓN DEL COMANDO SQL
                // ==============================================================================

                // Crear comando SQL asociado a la conexión abierta
                // SqlCommand encapsula la consulta SQL y parámetros (si los hubiera)
                using var comando = new SqlCommand(consultaSql, conexion);

                // Ejecutar consulta de forma asíncrona y obtener SqlDataReader
                // SqlDataReader permite leer los resultados fila por fila de manera eficiente
                using var lector = await comando.ExecuteReaderAsync();

                // ==============================================================================
                // FASE 8: PROCESAMIENTO DE RESULTADOS
                // ==============================================================================

                // Leer cada fila devuelta por la consulta SQL
                // ReadAsync() devuelve true mientras haya más filas que leer
                while (await lector.ReadAsync())
                {
                    // Crear diccionario para representar la fila actual
                    // Cada fila será un Dictionary<string, object?> donde:
                    // - string = nombre de columna
                    // - object? = valor de la celda (puede ser null)
                    var fila = new Dictionary<string, object?>();

                    // Iterar sobre todas las columnas de la fila actual
                    // FieldCount es el número total de columnas en el resultado
                    for (int indiceColumna = 0; indiceColumna < lector.FieldCount; indiceColumna++)
                    {
                        // Obtener nombre de columna tal como está definido en SQL Server
                        // Mantiene mayúsculas/minúsculas y caracteres especiales originales
                        string nombreColumna = lector.GetName(indiceColumna);

                        // Obtener valor de la celda con conversión apropiada de tipos
                        // Conversión crítica: DBNull.Value (SQL Server) → null (C#)
                        // SQL Server usa DBNull.Value para representar valores NULL
                        // C# usa null, entonces necesitamos esta conversión explícita
                        object? valorColumna = lector.IsDBNull(indiceColumna)
                            ? null                          // Convertir DBNull a null C#
                            : lector.GetValue(indiceColumna); // Obtener valor real con tipo apropiado

                        // Agregar par clave-valor al diccionario de la fila
                        // La clave es el nombre de columna, el valor es el dato de la celda
                        fila[nombreColumna] = valorColumna;
                    }

                    // Agregar fila completa (como diccionario) a la lista de resultados
                    resultados.Add(fila);
                }

                // Aquí el using se encarga de cerrar automáticamente:
                // - SqlDataReader (lector)
                // - SqlCommand (comando)  
                // - SqlConnection (conexion)
            }
            catch (SqlException excepcionSql)
            {
                // ==============================================================================
                // MANEJO ESPECÍFICO DE ERRORES DE SQL SERVER CON FALLBACK A ESQUEMA POR DEFECTO
                // ==============================================================================

                // SqlException contiene códigos de error específicos de SQL Server:
                // - 208: Invalid object name (tabla o esquema no existe)
                // - 18: Login failed  
                // - 2: Timeout
                // - etc.

                // LÓGICA DE FALLBACK: Si esquema no existe, intentar con "dbo"
                // Error 208 significa "Invalid object name" - típicamente esquema o tabla no existe
                // Solo aplicar fallback si se especificó un esquema diferente a "dbo"
                if (excepcionSql.Number == 208 && !string.IsNullOrWhiteSpace(esquema) && !esquema.Equals("dbo", StringComparison.OrdinalIgnoreCase))
                {
                    // REINTENTAR AUTOMÁTICAMENTE CON ESQUEMA POR DEFECTO
                    // Esta lógica permite mayor tolerancia a errores de configuración
                    // El usuario obtiene resultados aunque haya especificado esquema incorrecto
                    try
                    {
                        // Llamada recursiva usando "dbo" como esquema
                        // Mantiene los mismos parámetros (tabla y límite) pero cambia esquema
                        return await ObtenerFilasAsync(nombreTabla, "dbo", limite);
                    }
                    catch
                    {
                        // Si también falla con esquema "dbo", entonces la tabla realmente no existe
                        // Lanzar error informativo que explique qué se intentó hacer
                        throw new InvalidOperationException(
                            $"Error SQL: La tabla '{esquema}.{nombreTabla}' no existe en el esquema especificado. " +
                            $"Se intentó automáticamente con esquema por defecto 'dbo.{nombreTabla}' pero tampoco existe. " +
                            $"Verificar que la tabla '{nombreTabla}' existe en la base de datos y en qué esquema está ubicada.",
                            excepcionSql  // Mantener excepción original como InnerException para debugging
                        );
                    }
                }

                // ERROR NORMAL - No relacionado con esquema incorrecto o ya se usó "dbo"
                // Re-lanzar como InvalidOperationException con contexto adicional para facilitar debugging
                // Incluir información específica que ayude al desarrollador a resolver el problema
                throw new InvalidOperationException(
                    $"Error de SQL Server al consultar la tabla '{esquemaFinal}.{nombreTabla}': {excepcionSql.Message}. " +
                    $"Código de error SQL Server: {excepcionSql.Number}. " +
                    $"Verificar que la tabla existe y se tienen permisos de lectura.",
                    excepcionSql  // Mantener excepción original como InnerException
                );
            }
            catch (Exception excepcionGeneral)
            {
                // ==============================================================================
                // MANEJO DE ERRORES INESPERADOS
                // ==============================================================================

                // Capturar cualquier otro error que no sea específico de SQL Server
                // Ejemplos: errores de red, OutOfMemoryException, etc.
                throw new InvalidOperationException(
                    $"Error inesperado al acceder a SQL Server para tabla '{esquemaFinal}.{nombreTabla}': {excepcionGeneral.Message}. " +
                    $"Verificar conectividad y configuración del servidor.",
                    excepcionGeneral  // Mantener excepción original como InnerException
                );
            }

            // ==============================================================================
            // FASE 9: RETORNO DE RESULTADOS
            // ==============================================================================

            // Devolver como IReadOnlyList para cumplir el contrato de la interface
            // y prevenir modificaciones accidentales por parte del código cliente
            // La lista puede estar vacía (Count = 0) pero nunca null
            return resultados;
        }

        /// <summary>
        /// Implementa la consulta filtrada por clave específica para SQL Server.
        /// 
        /// Construye y ejecuta consulta SQL con WHERE parametrizado:
        /// SELECT * FROM [esquema].[tabla] WHERE [columna] = @valor
        /// 
        /// Diferencias con ObtenerFilasAsync:
        /// - Agrega cláusula WHERE con parámetro para filtrar por columna específica
        /// - Usa SqlParameter para prevenir inyección SQL
        /// - No aplica límite TOP porque espera pocos resultados por filtro específico
        /// </summary>
        /// <param name="nombreTabla">Nombre de la tabla a consultar</param>
        /// <param name="esquema">Esquema de la tabla (null = "dbo" por defecto)</param>
        /// <param name="nombreClave">Nombre de la columna para el filtro WHERE</param>
        /// <param name="valor">Valor exacto a buscar en la columna especificada</param>
        /// <returns>Lista de filas que coinciden exactamente con el criterio de filtrado</returns>
        public async Task<IReadOnlyList<Dictionary<string, object?>>> ObtenerPorClaveAsync(
            string nombreTabla,     // Tabla objetivo para la consulta filtrada
            string? esquema,        // Esquema opcional (null = usar "dbo" por defecto)
            string nombreClave,     // Columna para aplicar filtro WHERE
            string valor           // Valor específico a buscar (se usa como parámetro SQL)
        )
        {
            // ==============================================================================
            // FASE 1: VALIDACIONES DE ENTRADA ESPECÍFICAS PARA CONSULTA FILTRADA
            // ==============================================================================

            // Validar tabla (igual que en ObtenerFilasAsync)
            if (string.IsNullOrWhiteSpace(nombreTabla))
                throw new ArgumentException("El nombre de la tabla no puede estar vacío.", nameof(nombreTabla));

            // NUEVA VALIDACIÓN: nombre de columna para el filtro WHERE
            // Sin esta validación se construiría SQL inválido: WHERE [] = @valor
            if (string.IsNullOrWhiteSpace(nombreClave))
                throw new ArgumentException("El nombre de la clave no puede estar vacío.", nameof(nombreClave));

            // NUEVA VALIDACIÓN: valor a buscar
            // Permitir valores como "0" o " " que podrían ser datos válidos en la BD
            if (string.IsNullOrWhiteSpace(valor))
                throw new ArgumentException("El valor no puede estar vacío.", nameof(valor));

            // ==============================================================================
            // FASE 2: NORMALIZACIÓN (IGUAL QUE ObtenerFilasAsync)
            // ==============================================================================

            // Aplicar esquema por defecto de SQL Server si no se especifica
            string esquemaFinal = string.IsNullOrWhiteSpace(esquema) ? "dbo" : esquema.Trim();

            // ==============================================================================
            // FASE 3: CONSTRUCCIÓN SQL PARAMETRIZADA (DIFERENCIA CLAVE)
            // ==============================================================================

            // Construir consulta SQL con WHERE parametrizado para prevenir inyección SQL
            // IMPORTANTE: No usar concatenación de strings para valores del usuario
            // MAL:  "WHERE columna = '" + valor + "'"  ← Vulnerable a SQL injection
            // BIEN: "WHERE columna = @valor"           ← Seguro con SqlParameter
            string consultaSql = $"SELECT * FROM [{esquemaFinal}].[{nombreTabla}] WHERE [{nombreClave}] = @valor";

            // Nota: No usamos TOP aquí porque esperamos pocos resultados al filtrar por clave específica
            // Si se necesita límite en el futuro, se puede agregar como parámetro opcional

            // ==============================================================================
            // FASE 4: PREPARACIÓN DE ESTRUCTURAS (IGUAL QUE ObtenerFilasAsync)
            // ==============================================================================

            // Lista mutable para recolectar resultados durante el procesamiento
            var resultados = new List<Dictionary<string, object?>>();

            try
            {
                // ==============================================================================
                // FASE 5: CONEXIÓN A SQL SERVER (IGUAL QUE ObtenerFilasAsync)
                // ==============================================================================

                // Obtener cadena de conexión vía DIP
                string cadenaConexion = _proveedorConexion.ObtenerCadenaConexion();

                // Crear y abrir conexión con using para liberación automática de recursos
                using var conexion = new SqlConnection(cadenaConexion);
                await conexion.OpenAsync();

                // ==============================================================================
                // FASE 6: COMANDO SQL CON PARÁMETROS (DIFERENCIA PRINCIPAL)
                // ==============================================================================

                // Crear comando SQL asociado a la conexión
                using var comando = new SqlCommand(consultaSql, conexion);

                // CRÍTICO: Agregar parámetro para prevenir inyección SQL
                // SqlParameter maneja automáticamente:
                // - Escape de caracteres especiales (comillas, etc.)
                // - Conversión de tipos apropiada
                // - Prevención de inyección SQL maliciosa
                comando.Parameters.AddWithValue("@valor", valor);

                // ==============================================================================
                // FASE 7: EJECUCIÓN Y PROCESAMIENTO (IGUAL QUE ObtenerFilasAsync)
                // ==============================================================================

                // Ejecutar consulta y procesar resultados
                using var lector = await comando.ExecuteReaderAsync();

                // Procesar cada fila encontrada (mismo patrón que ObtenerFilasAsync)
                while (await lector.ReadAsync())
                {
                    // Crear diccionario para la fila actual
                    var fila = new Dictionary<string, object?>();

                    // Mapear cada columna de la fila a par clave-valor en el diccionario
                    for (int indiceColumna = 0; indiceColumna < lector.FieldCount; indiceColumna++)
                    {
                        // Obtener nombre de columna tal como está en SQL Server
                        string nombreColumna = lector.GetName(indiceColumna);

                        // Convertir DBNull.Value (SQL Server) a null (C#)
                        object? valorColumna = lector.IsDBNull(indiceColumna)
                            ? null
                            : lector.GetValue(indiceColumna);

                        // Agregar par clave-valor al diccionario de la fila
                        fila[nombreColumna] = valorColumna;
                    }

                    // Agregar fila procesada a la lista de resultados
                    resultados.Add(fila);
                }

                // Los using se encargan de liberar recursos automáticamente:
                // - SqlDataReader, SqlCommand, SqlConnection
            }
            catch (SqlException excepcionSql)
            {
                // ==============================================================================
                // MANEJO ESPECÍFICO DE ERRORES SQL CON CONTEXTO DE FILTRADO
                // ==============================================================================

                // Proporcionar contexto específico del filtrado para facilitar debugging
                // Incluir información sobre qué filtro se estaba aplicando cuando falló
                throw new InvalidOperationException(
                    $"Error SQL al filtrar tabla '{esquemaFinal}.{nombreTabla}' por columna '{nombreClave}' con valor '{valor}': {excepcionSql.Message}. " +
                    $"Verificar que la columna existe y el tipo de dato es compatible.",
                    excepcionSql
                );
            }
            catch (Exception excepcionGeneral)
            {
                // Manejo de errores generales con contexto del filtrado
                throw new InvalidOperationException(
                    $"Error inesperado al filtrar tabla '{esquemaFinal}.{nombreTabla}' por {nombreClave}='{valor}': {excepcionGeneral.Message}",
                    excepcionGeneral
                );
            }

            // ==============================================================================
            // FASE 8: RETORNO DE RESULTADOS FILTRADOS
            // ==============================================================================

            // Devolver como IReadOnlyList para cumplir contrato de la interface
            // La lista puede estar vacía si no se encontraron coincidencias (normal)
            // Nunca devuelve null, siempre lista (vacía o con datos)
            return resultados;
        }


        /// <summary>
        /// Implementa la inserción de registro para SQL Server con soporte para encriptación BCrypt.
        /// Construye y ejecuta: INSERT INTO [esquema].[tabla] (columnas) VALUES (@param1, @param2, ...)
        /// </summary>
        public async Task<bool> CrearAsync(
            string nombreTabla,
            string? esquema,
            Dictionary<string, object?> datos,
            string? camposEncriptar = null
        )
        {
            // ==============================================================================
            // FASE 1: VALIDACIONES DE ENTRADA
            // ==============================================================================

            if (string.IsNullOrWhiteSpace(nombreTabla))
                throw new ArgumentException("El nombre de la tabla no puede estar vacío.", nameof(nombreTabla));

            if (datos == null || !datos.Any())
                throw new ArgumentException("Los datos no pueden estar vacíos.", nameof(datos));

            // ==============================================================================
            // FASE 2: NORMALIZACIÓN DE PARÁMETROS
            // ==============================================================================

            string esquemaFinal = string.IsNullOrWhiteSpace(esquema) ? "dbo" : esquema.Trim();

            // ==============================================================================
            // FASE 3: PROCESAMIENTO DE ENCRIPTACIÓN CON BCRYPT
            // ==============================================================================

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

            // ==============================================================================
            // FASE 4: CONSTRUCCIÓN DE CONSULTA SQL PARAMETRIZADA
            // ==============================================================================

            // Construir listas de columnas y parámetros para la consulta INSERT
            var columnas = string.Join(", ", datosFinales.Keys.Select(k => $"[{k}]"));
            var parametros = string.Join(", ", datosFinales.Keys.Select(k => $"@{k}"));

            string consultaSql = $"INSERT INTO [{esquemaFinal}].[{nombreTabla}] ({columnas}) VALUES ({parametros})";

            try
            {
                // ==============================================================================
                // FASE 5: CONEXIÓN Y EJECUCIÓN SQL
                // ==============================================================================

                string cadenaConexion = _proveedorConexion.ObtenerCadenaConexion();

                using var conexion = new SqlConnection(cadenaConexion);
                await conexion.OpenAsync();

                using var comando = new SqlCommand(consultaSql, conexion);

                // Agregar todos los parámetros a la consulta SQL
                foreach (var kvp in datosFinales)
                {
                    // Convertir null de C# a DBNull.Value de SQL Server
                    comando.Parameters.AddWithValue($"@{kvp.Key}", kvp.Value ?? DBNull.Value);
                }

                // Ejecutar inserción y verificar que se insertó al menos un registro
                int filasAfectadas = await comando.ExecuteNonQueryAsync();
                return filasAfectadas > 0;
            }
            catch (SqlException excepcionSql)
            {
                // Manejo específico de errores de SQL Server con contexto de inserción
                throw new InvalidOperationException(
                    $"Error SQL al insertar en tabla '{esquemaFinal}.{nombreTabla}': {excepcionSql.Message}. " +
                    $"Código de error: {excepcionSql.Number}. " +
                    $"Verificar que la tabla existe, las columnas son correctas y no hay violaciones de restricciones.",
                    excepcionSql
                );
            }
            catch (Exception excepcionGeneral)
            {
                throw new InvalidOperationException(
                    $"Error inesperado al insertar en '{esquemaFinal}.{nombreTabla}': {excepcionGeneral.Message}",
                    excepcionGeneral
                );
            }
        }

        /// <summary>
        /// Implementa la actualización de registro para SQL Server con soporte para encriptación BCrypt.
        /// Construye y ejecuta: UPDATE [esquema].[tabla] SET col1=@val1, col2=@val2 WHERE [clave]=@valorClave
        /// </summary>
        public async Task<int> ActualizarAsync(
            string nombreTabla,
            string? esquema,
            string nombreClave,
            string valorClave,
            Dictionary<string, object?> datos,
            string? camposEncriptar = null
        )
        {
            // ==============================================================================
            // FASE 1: VALIDACIONES DE ENTRADA
            // ==============================================================================

            if (string.IsNullOrWhiteSpace(nombreTabla))
                throw new ArgumentException("El nombre de la tabla no puede estar vacío.", nameof(nombreTabla));

            if (string.IsNullOrWhiteSpace(nombreClave))
                throw new ArgumentException("El nombre de la clave no puede estar vacío.", nameof(nombreClave));

            if (string.IsNullOrWhiteSpace(valorClave))
                throw new ArgumentException("El valor de la clave no puede estar vacío.", nameof(valorClave));

            if (datos == null || !datos.Any())
                throw new ArgumentException("Los datos a actualizar no pueden estar vacíos.", nameof(datos));

            // ==============================================================================
            // FASE 2: NORMALIZACIÓN DE PARÁMETROS
            // ==============================================================================

            string esquemaFinal = string.IsNullOrWhiteSpace(esquema) ? "dbo" : esquema.Trim();

            // ==============================================================================
            // FASE 3: PROCESAMIENTO DE ENCRIPTACIÓN CON BCRYPT
            // ==============================================================================

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

                        // Usar clase de utilidad BCrypt para encriptar
                        datosFinales[campo] = webapicsharp.Servicios.Utilidades.EncriptacionBCrypt.Encriptar(valorOriginal);
                    }
                }
            }

            // ==============================================================================
            // FASE 4: CONSTRUCCIÓN DE CONSULTA SQL UPDATE PARAMETRIZADA
            // ==============================================================================

            // Construir cláusula SET con parámetros para cada columna a actualizar
            var clausulaSet = string.Join(", ", datosFinales.Keys.Select(k => $"[{k}] = @{k}"));

            // Construir consulta UPDATE completa con WHERE parametrizado
            string consultaSql = $"UPDATE [{esquemaFinal}].[{nombreTabla}] SET {clausulaSet} WHERE [{nombreClave}] = @valorClave";

            try
            {
                // ==============================================================================
                // FASE 5: CONEXIÓN Y EJECUCIÓN SQL
                // ==============================================================================

                string cadenaConexion = _proveedorConexion.ObtenerCadenaConexion();

                using var conexion = new SqlConnection(cadenaConexion);
                await conexion.OpenAsync();

                using var comando = new SqlCommand(consultaSql, conexion);

                // Agregar parámetros para las columnas a actualizar (cláusula SET)
                foreach (var kvp in datosFinales)
                {
                    comando.Parameters.AddWithValue($"@{kvp.Key}", kvp.Value ?? DBNull.Value);
                }

                // Agregar parámetro para la cláusula WHERE
                comando.Parameters.AddWithValue("@valorClave", valorClave);

                // Ejecutar UPDATE y obtener número de filas afectadas
                // Si retorna 0, significa que no se encontró el registro
                // Si retorna >0, indica cuántos registros se actualizaron
                int filasAfectadas = await comando.ExecuteNonQueryAsync();
                return filasAfectadas;
            }
            catch (SqlException excepcionSql)
            {
                // Manejo específico de errores de SQL Server con contexto de actualización
                throw new InvalidOperationException(
                    $"Error SQL al actualizar tabla '{esquemaFinal}.{nombreTabla}' WHERE {nombreClave}='{valorClave}': {excepcionSql.Message}. " +
                    $"Código de error: {excepcionSql.Number}. " +
                    $"Verificar que la tabla y columnas existen, y no hay violaciones de restricciones.",
                    excepcionSql
                );
            }
            catch (Exception excepcionGeneral)
            {
                throw new InvalidOperationException(
                    $"Error inesperado al actualizar '{esquemaFinal}.{nombreTabla}' WHERE {nombreClave}='{valorClave}': {excepcionGeneral.Message}",
                    excepcionGeneral
                );
            }
        }

        /// <summary>
        /// Implementa la eliminación de registro para SQL Server.
        /// Construye y ejecuta: DELETE FROM [esquema].[tabla] WHERE [clave]=@valorClave
        /// </summary>
        public async Task<int> EliminarAsync(
            string nombreTabla,
            string? esquema,
            string nombreClave,
            string valorClave
        )
        {
            // ==============================================================================
            // FASE 1: VALIDACIONES DE ENTRADA
            // ==============================================================================

            if (string.IsNullOrWhiteSpace(nombreTabla))
                throw new ArgumentException("El nombre de la tabla no puede estar vacío.", nameof(nombreTabla));

            if (string.IsNullOrWhiteSpace(nombreClave))
                throw new ArgumentException("El nombre de la clave no puede estar vacío.", nameof(nombreClave));

            if (string.IsNullOrWhiteSpace(valorClave))
                throw new ArgumentException("El valor de la clave no puede estar vacío.", nameof(valorClave));

            // ==============================================================================
            // FASE 2: NORMALIZACIÓN DE PARÁMETROS
            // ==============================================================================

            string esquemaFinal = string.IsNullOrWhiteSpace(esquema) ? "dbo" : esquema.Trim();

            // ==============================================================================
            // FASE 3: CONSTRUCCIÓN DE CONSULTA SQL DELETE PARAMETRIZADA
            // ==============================================================================

            // Construir consulta DELETE con WHERE parametrizado para prevenir SQL injection
            string consultaSql = $"DELETE FROM [{esquemaFinal}].[{nombreTabla}] WHERE [{nombreClave}] = @valorClave";

            try
            {
                // ==============================================================================
                // FASE 4: CONEXIÓN Y EJECUCIÓN SQL
                // ==============================================================================

                string cadenaConexion = _proveedorConexion.ObtenerCadenaConexion();

                using var conexion = new SqlConnection(cadenaConexion);
                await conexion.OpenAsync();

                using var comando = new SqlCommand(consultaSql, conexion);

                // Agregar parámetro para la cláusula WHERE
                comando.Parameters.AddWithValue("@valorClave", valorClave);

                // Ejecutar DELETE y obtener número de filas eliminadas
                // Si retorna 0, significa que no se encontró el registro
                // Si retorna >0, indica cuántos registros se eliminaron
                int filasEliminadas = await comando.ExecuteNonQueryAsync();
                return filasEliminadas;
            }
            catch (SqlException excepcionSql)
            {
                // Manejo específico de errores de SQL Server con contexto de eliminación
                throw new InvalidOperationException(
                    $"Error SQL al eliminar de tabla '{esquemaFinal}.{nombreTabla}' WHERE {nombreClave}='{valorClave}': {excepcionSql.Message}. " +
                    $"Código de error: {excepcionSql.Number}. " +
                    $"Verificar que la tabla existe y no hay restricciones de clave foránea.",
                    excepcionSql
                );
            }
            catch (Exception excepcionGeneral)
            {
                throw new InvalidOperationException(
                    $"Error inesperado al eliminar de '{esquemaFinal}.{nombreTabla}' WHERE {nombreClave}='{valorClave}': {excepcionGeneral.Message}",
                    excepcionGeneral
                );
            }
        }
        /// <summary>
        /// Implementa la obtención de hash de contraseña para SQL Server.
        /// Construye y ejecuta: SELECT [campoContrasena] FROM [esquema].[tabla] WHERE [campoUsuario]=@valorUsuario
        /// </summary>
        public async Task<string?> ObtenerHashContrasenaAsync(
            string nombreTabla,
            string? esquema,
            string campoUsuario,
            string campoContrasena,
            string valorUsuario
        )
        {
            // ==============================================================================
            // FASE 1: VALIDACIONES DE ENTRADA
            // ==============================================================================

            if (string.IsNullOrWhiteSpace(nombreTabla))
                throw new ArgumentException("El nombre de la tabla no puede estar vacío.", nameof(nombreTabla));

            if (string.IsNullOrWhiteSpace(campoUsuario))
                throw new ArgumentException("El campo de usuario no puede estar vacío.", nameof(campoUsuario));

            if (string.IsNullOrWhiteSpace(campoContrasena))
                throw new ArgumentException("El campo de contraseña no puede estar vacío.", nameof(campoContrasena));

            if (string.IsNullOrWhiteSpace(valorUsuario))
                throw new ArgumentException("El valor de usuario no puede estar vacío.", nameof(valorUsuario));

            // ==============================================================================
            // FASE 2: NORMALIZACIÓN DE PARÁMETROS
            // ==============================================================================

            string esquemaFinal = string.IsNullOrWhiteSpace(esquema) ? "dbo" : esquema.Trim();

            // ==============================================================================
            // FASE 3: CONSTRUCCIÓN DE CONSULTA SQL SELECT ESPECÍFICA
            // ==============================================================================

            // Construir consulta SELECT que obtiene solo el hash de contraseña del usuario específico
            // Solo seleccionamos la columna de contraseña para minimizar datos transferidos
            string consultaSql = $"SELECT [{campoContrasena}] FROM [{esquemaFinal}].[{nombreTabla}] WHERE [{campoUsuario}] = @valorUsuario";

            try
            {
                // ==============================================================================
                // FASE 4: CONEXIÓN Y EJECUCIÓN SQL
                // ==============================================================================

                string cadenaConexion = _proveedorConexion.ObtenerCadenaConexion();

                using var conexion = new SqlConnection(cadenaConexion);
                await conexion.OpenAsync();

                using var comando = new SqlCommand(consultaSql, conexion);

                // Agregar parámetro para identificar al usuario
                comando.Parameters.AddWithValue("@valorUsuario", valorUsuario);

                // Usar ExecuteScalarAsync para obtener un solo valor (el hash de contraseña)
                var resultado = await comando.ExecuteScalarAsync();

                // Convertir resultado a string o null si no se encontró el usuario
                return resultado?.ToString();
            }
            catch (SqlException excepcionSql)
            {
                // Manejo específico de errores de SQL Server
                throw new InvalidOperationException(
                    $"Error SQL al obtener hash de contraseña de tabla '{esquemaFinal}.{nombreTabla}' WHERE {campoUsuario}='{valorUsuario}': {excepcionSql.Message}. " +
                    $"Código de error: {excepcionSql.Number}",
                    excepcionSql
                );
            }
            catch (Exception excepcionGeneral)
            {
                throw new InvalidOperationException(
                    $"Error inesperado al obtener hash de contraseña de '{esquemaFinal}.{nombreTabla}': {excepcionGeneral.Message}",
                    excepcionGeneral
                );
            }
        }
        // otras posibles operaciones específicas de SQL Server se pueden agregar aquí


    }
}

// NOTAS PEDAGÓGICAS para el tutorial:
//
// 1. ESTA ES UNA IMPLEMENTACIÓN CONCRETA (NO ABSTRACCIÓN):
//    - Implementa la interface IRepositorioLecturaTabla definida en Abstracciones/
//    - Define CÓMO leer datos específicamente de SQL Server
//    - Contiene lógica específica del proveedor: sintaxis SQL, tipos de datos, excepciones
//    - Es una de varias posibles implementaciones (PostgreSQL, MariaDB, Oracle, etc.)
//
// 2. APLICACIÓN PRÁCTICA DE DIP (DEPENDENCY INVERSION PRINCIPLE):
//    - Depende de IProveedorConexion (abstracción), no de ProveedorConexion (implementación)
//    - No sabe cómo se obtienen las cadenas de conexión, solo las solicita
//    - Permite cambiar la forma de obtener conexiones sin modificar este código
//    - Facilita testing: se puede inyectar un IProveedorConexion mock para pruebas
//
// 3. SEPARACIÓN CLARA DE RESPONSABILIDADES (SRP):
//    - Solo se encarga de acceso a datos de SQL Server (una responsabilidad)
//    - NO contiene validaciones de negocio (eso va en la capa de servicios)
//    - NO maneja formato de respuestas HTTP (eso va en la capa de controladores)
//    - NO maneja autenticación/autorización (eso va en middleware)
//
// 4. CARACTERÍSTICAS ESPECÍFICAS DE SQL SERVER:
//    - Esquema por defecto: "dbo" (Database Owner)
//    - Sintaxis de límite: TOP (no LIMIT como PostgreSQL/MySQL)
//    - Escapado de nombres: corchetes [] (no comillas "" como PostgreSQL)
//    - Conversión de tipos: DBNull.Value → null
//    - Excepciones específicas: SqlException con códigos numéricos
//
// 5. ¿QUÉ VIENE DESPUÉS EN EL TUTORIAL?
//    - Crear implementaciones para otros proveedores (PostgreSQL, MariaDB)
//    - Instalar paquete NuGet: dotnet add package Microsoft.Data.SqlClient
//    - Registrar en Program.cs: AddScoped<IRepositorioLecturaTabla, RepositorioLecturaSqlServer>
//    - Crear servicios de negocio que usen esta interface
//
// 6. PRÓXIMO PASO EN PROGRAM.CS:
//    Descomentar la línea correspondiente en el switch según el proveedor configurado:
//    case "sqlserver":
//    case "sqlserverexpress": 
//    case "localdb":
//    default:
//        builder.Services.AddScoped<webapicsharp.Repositorios.Abstracciones.IRepositorioLecturaTabla,
//                                   webapicsharp.Repositorios.RepositorioLecturaSqlServer>();
//        break;
//
// 7. ¿POR QUÉ ADDSCOPED Y NO SINGLETON?
//    - Scoped: una instancia nueva por cada petición HTTP (se destruye al finalizar)
//    - Los repositorios pueden mantener estado de conexión/transacción específico del request
//    - Mejor aislamiento: cada petición tiene su propia instancia de repositorio
//    - Previene problemas de concurrencia en conexiones de base de datos
//    - Consumo de memoria controlado: se liberan recursos al final de cada request
//
// 8. TESTING DE ESTA CLASE:
//    Esta clase es fácil de testear unitariamente porque:
//    - Recibe IProveedorConexion que se puede mockear fácilmente
//    - Los métodos son async y devuelven tipos concretos y verificables
//    - Los errores están bien categorizados (ArgumentException vs InvalidOperationException)
//    - No tiene dependencias estáticas o globales
//
//    Ejemplo de test:
//    var mockProveedor = new Mock<IProveedorConexion>();
//    mockProveedor.Setup(p => p.ObtenerCadenaConexion()).Returns("test connection string");
//    var repo = new RepositorioLecturaSqlServer(mockProveedor.Object);
//    // ... configurar base de datos de prueba y ejecutar asserts
//
// 9. CONSIDERACIONES DE RENDIMIENTO:
//    - Uso de async/await para no bloquear threads del pool
//    - Using statements para liberación automática de recursos
//    - TOP en SQL para limitar resultados a nivel de base de datos (eficiente)
//    - SqlDataReader para lectura secuencial eficiente de resultados
//    - Conexiones de corta duración (se abren y cierran por operación)
//
// 10. EXTENSIBILIDAD FUTURA:
//     Esta implementación se puede extender para:
//     - Agregar métodos de escritura (INSERT, UPDATE, DELETE)
//     - Implementar paginación avanzada (OFFSET/FETCH)
//     - Agregar soporte para procedimientos almacenados
//     - Implementar connection pooling personalizado
//     - Agregar logging detallado de consultas ejecutadas
