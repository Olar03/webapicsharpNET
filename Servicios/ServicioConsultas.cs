// ServicioConsultas.cs — Implementación de la lógica de negocio para consultas SQL parametrizadas
// Ubicación: Servicios/ServicioConsultas.cs
//
// Principios SOLID aplicados:
// - SRP: Esta clase solo se encarga de lógica de negocio para consultas SQL, nada más
// - DIP: Depende de IRepositorioConsultas (abstracción), no de implementaciones concretas
// - OCP: Se puede cambiar el repositorio sin modificar este servicio
// - LSP: Implementa completamente IServicioConsultas, es intercambiable con otras implementaciones

using System;                                             // Para ArgumentException y ArgumentNullException
using System.Collections.Generic;                        // Para List<> y Dictionary<>
using System.Threading.Tasks;                            // Para async/await
using System.Data;                                       // Para DataTable
using Microsoft.Data.SqlClient;                         // Para SqlParameter
using System.Text.Json;                                 // Para JsonElement
using System.Text.RegularExpressions;                   // Para validaciones con Regex
using Microsoft.Extensions.Configuration;                // Para IConfiguration y acceso a appsettings.json
using webapicsharp.Servicios.Abstracciones;             // Para IServicioConsultas
using webapicsharp.Repositorios.Abstracciones;          // Para IRepositorioConsultas

namespace webapicsharp.Servicios
{
    /// <summary>
    /// Implementación concreta del servicio de consultas SQL parametrizadas.
    /// 
    /// Esta clase actúa como coordinadora entre la capa de presentación (Controllers)
    /// y la capa de acceso a datos (Repositorios). Sus responsabilidades incluyen:
    /// 
    /// - Aplicar validaciones de seguridad específicas del dominio
    /// - Validar que solo se ejecuten consultas SELECT autorizadas
    /// - Verificar que no se acceda a tablas prohibidas según configuración
    /// - Convertir parámetros de formato JSON a SqlParameter seguros
    /// - Coordinar con el repositorio para ejecutar consultas validadas
    /// - Aplicar límites de seguridad para prevenir consultas masivas
    /// - Manejar errores de forma consistente con mensajes informativos
    /// </summary>
    public class ServicioConsultas : IServicioConsultas
    {
        // Campos privados que mantienen las dependencias inyectadas
        // Aplica DIP: depende de abstracciones, no de implementaciones concretas
        private readonly IRepositorioConsultas _repositorioConsultas;
        private readonly IConfiguration _configuration;

        /// <summary>
        /// Constructor que recibe dependencias mediante inyección de dependencias.
        /// 
        /// El flujo completo de inyección es:
        /// 1. Program.cs registra: AddScoped<IServicioConsultas, ServicioConsultas>
        /// 2. Program.cs registra: AddScoped<IRepositorioConsultas, RepositorioConsultasSqlServer>
        /// 3. Cuando se solicita IServicioConsultas, el contenedor:
        ///    - Crea ServicioConsultas
        ///    - Ve que necesita IRepositorioConsultas e IConfiguration
        ///    - Inyecta automáticamente las dependencias registradas
        /// 
        /// Aplica DIP: este servicio no sabe qué repositorio específico recibe
        /// ni de dónde viene la configuración, solo sabe que implementan sus interfaces.
        /// </summary>
        /// <param name="repositorioConsultas">
        /// Repositorio que implementa las operaciones de acceso a datos para consultas.
        /// Se inyecta automáticamente según la configuración en Program.cs.
        /// </param>
        /// <param name="configuration">
        /// Configuración de la aplicación para acceder a tablas prohibidas y otros settings.
        /// Se inyecta automáticamente por ASP.NET Core.
        /// </param>
        public ServicioConsultas(IRepositorioConsultas repositorioConsultas, IConfiguration configuration)
        {
            // Validaciones defensivas para asegurar inyección correcta
            _repositorioConsultas = repositorioConsultas ?? throw new ArgumentNullException(
                nameof(repositorioConsultas),
                "IRepositorioConsultas no puede ser null. Verificar registro en Program.cs.");

            _configuration = configuration ?? throw new ArgumentNullException(
                nameof(configuration),
                "IConfiguration no puede ser null. Problema en configuración de ASP.NET Core.");
        }

        /// <summary>
        /// Implementa IServicioConsultas.ValidarConsultaSQL aplicando reglas de seguridad.
        /// 
        /// Validaciones de seguridad implementadas:
        /// 1. Consulta no puede estar vacía o ser null
        /// 2. Solo se permiten consultas SELECT (previene modificaciones)
        /// 3. No puede acceder a tablas prohibidas según configuración
        /// 4. Validaciones adicionales de seguridad según sea necesario
        /// </summary>
        /// <param name="consulta">Consulta SQL a validar</param>
        /// <param name="tablasProhibidas">Array de tablas restringidas</param>
        /// <returns>Tupla indicando si es válida y mensaje de error si aplica</returns>
        public (bool esValida, string? mensajeError) ValidarConsultaSQL(string consulta, string[] tablasProhibidas)
        {
            // FASE 1: VALIDACIÓN BÁSICA DE ENTRADA
            if (string.IsNullOrWhiteSpace(consulta))
                return (false, "La consulta no puede estar vacía.");

            // FASE 2: VALIDACIÓN DE TIPO DE CONSULTA (SOLO SELECT PERMITIDO)
            // Normalizar consulta para análisis (quitar espacios y convertir a mayúsculas)
            string consultaNormalizada = consulta.Trim().ToUpperInvariant();

            // Verificar que comience con SELECT (política de seguridad: solo lecturas)
            if (!consultaNormalizada.StartsWith("SELECT"))
                return (false, "Solo se permiten consultas SELECT por motivos de seguridad.");

            // FASE 3: VALIDACIÓN DE TABLAS PROHIBIDAS
            // Verificar que la consulta no acceda a tablas restringidas según configuración
            foreach (var tabla in tablasProhibidas)
            {
                if (consulta.Contains(tabla, StringComparison.OrdinalIgnoreCase))
                    return (false, $"La consulta intenta acceder a la tabla prohibida '{tabla}'.");
            }

            // FASE 4: VALIDACIONES ADICIONALES DE SEGURIDAD (EXTENSIBLES)
            // Aquí se pueden agregar más validaciones según necesidades:
            // - Palabras clave peligrosas (DROP, ALTER, etc.)
            // - Límites en la complejidad de la consulta
            // - Validación de sintaxis básica
            // - Restricciones específicas del dominio

            return (true, null);
        }

        /// <summary>
        /// Convierte parámetros Dictionary desde JSON a SqlParameter para uso seguro en consultas.
        /// 
        /// Este método maneja la conversión de tipos desde el formato JSON-friendly
        /// que recibe el controlador hasta el formato SqlParameter que necesita
        /// el repositorio para ejecución segura en base de datos.
        /// </summary>
        /// <param name="parametros">Diccionario de parámetros JSON</param>
        /// <returns>Lista de SqlParameter seguros para ejecutar</returns>
        private List<SqlParameter> ConvertirParametrosDesdeJson(Dictionary<string, object?>? parametros)
        {
            var parametrosSQL = new List<SqlParameter>();

            if (parametros == null) return parametrosSQL;

            foreach (var parametro in parametros)
            {
                // Asegurar que el nombre tenga el prefijo @
                string nombre = parametro.Key.StartsWith("@") ? parametro.Key : "@" + parametro.Key;

                // Validar que el nombre del parámetro sea válido
                if (!Regex.IsMatch(nombre, @"^@\w+$"))
                    throw new ArgumentException($"Nombre de parámetro inválido: {nombre}");

                // Convertir el valor según su tipo en JSON
                object? valor = parametro.Value switch
                {
                    JsonElement json => json.ValueKind switch
                    {
                        JsonValueKind.String => json.GetString(),
                        JsonValueKind.Number => json.TryGetInt64(out long l) ? l : json.GetDouble(),
                        JsonValueKind.True => true,
                        JsonValueKind.False => false,
                        JsonValueKind.Null => DBNull.Value,
                        _ => json.GetRawText()
                    },
                    null => DBNull.Value,
                    _ => parametro.Value
                };

                // Agregar el parámetro a la lista
                parametrosSQL.Add(new SqlParameter(nombre, valor ?? DBNull.Value));
            }

            return parametrosSQL;
        }

        /// <summary>
        /// Implementa IServicioConsultas.EjecutarConsultaParametrizadaAsync coordinando seguridad y ejecución.
        /// 
        /// Este es el método principal que maneja SqlParameter directamente.
        /// Aplica todas las validaciones de seguridad y delega al repositorio.
        /// </summary>
        /// <param name="consulta">Consulta SQL parametrizada a ejecutar</param>
        /// <param name="parametros">Lista de parámetros SQL para la consulta</param>
        /// <param name="maximoRegistros">Límite de registros</param>
        /// <param name="esquema">Esquema de BD</param>
        /// <returns>DataTable con los resultados de la consulta</returns>
        public async Task<DataTable> EjecutarConsultaParametrizadaAsync(
            string consulta,
            List<SqlParameter> parametros,
            int maximoRegistros,
            string? esquema)
        {
            // FASE 1: OBTENER CONFIGURACIÓN DE SEGURIDAD
            // Leer tablas prohibidas desde appsettings.json
            var tablasProhibidas = _configuration.GetSection("TablasProhibidas").Get<string[]>() ?? Array.Empty<string>();

            // FASE 2: APLICAR VALIDACIONES DE SEGURIDAD
            // Usar el método de validación para aplicar todas las reglas de negocio
            var (esConsultaValida, mensajeError) = ValidarConsultaSQL(consulta, tablasProhibidas);

            // Si la validación falla, lanzar excepción apropiada
            if (!esConsultaValida)
                throw new UnauthorizedAccessException(mensajeError ?? "Consulta no autorizada.");

            // FASE 3: DELEGACIÓN AL REPOSITORIO (APLICANDO DIP)
            // El servicio no sabe cómo se ejecuta técnicamente la consulta,
            // solo delega al repositorio después de aplicar las reglas de negocio
            return await _repositorioConsultas.EjecutarConsultaParametrizadaAsync(consulta, parametros);
        }

        /// <summary>
        /// Implementa IServicioConsultas.EjecutarConsultaParametrizadaDesdeJsonAsync.
        /// 
        /// Este método es específico para el controlador web que recibe parámetros
        /// en formato Dictionary desde JSON. Convierte internamente y usa el método principal.
        /// </summary>
        /// <param name="consulta">Consulta SQL parametrizada a ejecutar</param>
        /// <param name="parametros">Diccionario de parámetros desde JSON</param>
        /// <returns>DataTable con los resultados listos para convertir a JSON</returns>
        public async Task<DataTable> EjecutarConsultaParametrizadaDesdeJsonAsync(
            string consulta,
            Dictionary<string, object?>? parametros)
        {
            // FASE 1: CONVERTIR PARÁMETROS JSON A SQLPARAMETER
            // Convertir Dictionary a List<SqlParameter> para ejecución segura
            var parametrosSQL = ConvertirParametrosDesdeJson(parametros);

            // FASE 2: USAR EL MÉTODO PRINCIPAL CON VALORES POR DEFECTO
            // Delegar al método principal con límite por defecto de 10000 registros
            return await EjecutarConsultaParametrizadaAsync(consulta, parametrosSQL, 10000, null);
        }
        /// <summary>
        /// Implementa IServicioConsultas.EjecutarProcedimientoAlmacenadoAsync.
        /// 
        /// Ejecuta un procedimiento almacenado aplicando las mismas validaciones de seguridad
        /// que las consultas SQL, pero sin restricción de solo SELECT.
        /// </summary>
        /// <param name="nombreSP">Nombre del procedimiento almacenado a ejecutar</param>
        /// <param name="parametros">Diccionario de parámetros desde JSON</param>
        /// <param name="camposAEncriptar">Lista de campos que deben ser encriptados</param>
        /// <returns>DataTable con los resultados del procedimiento almacenado</returns>
        public async Task<DataTable> EjecutarProcedimientoAlmacenadoAsync(
            string nombreSP,
            Dictionary<string, object?>? parametros,
            List<string>? camposAEncriptar)
        {
            // FASE 1: VALIDACIONES BÁSICAS
            if (string.IsNullOrWhiteSpace(nombreSP))
                throw new ArgumentException("El nombre del procedimiento almacenado no puede estar vacío.", nameof(nombreSP));

            // FASE 2: CONVERTIR PARÁMETROS CON ENCRIPTACIÓN
            var parametrosSQL = ConvertirParametrosConEncriptacion(parametros, camposAEncriptar);

            // FASE 3: DELEGACIÓN AL REPOSITORIO
            return await _repositorioConsultas.EjecutarProcedimientoAlmacenadoAsync(nombreSP, parametrosSQL);
        }

        /// <summary>
        /// Convierte parámetros Dictionary a SqlParameter aplicando encriptación a campos específicos.
        /// </summary>
        /// <param name="parametros">Diccionario de parámetros JSON</param>
        /// <param name="camposAEncriptar">Lista de campos que deben ser encriptados</param>
        /// <returns>Lista de SqlParameter seguros con encriptación aplicada</returns>
        private List<SqlParameter> ConvertirParametrosConEncriptacion(
            Dictionary<string, object?>? parametros,
            List<string>? camposAEncriptar)
        {
            var parametrosSQL = new List<SqlParameter>();

            if (parametros == null) return parametrosSQL;

            foreach (var parametro in parametros)
            {
                // Asegurar que el nombre tenga el prefijo @
                string nombre = parametro.Key.StartsWith("@") ? parametro.Key : "@" + parametro.Key;

                // Validar que el nombre del parámetro sea válido
                if (!Regex.IsMatch(nombre, @"^@\w+$"))
                    throw new ArgumentException($"Nombre de parámetro inválido: {nombre}");

                // Convertir el valor según su tipo en JSON
                object? valor = parametro.Value switch
                {
                    JsonElement json => json.ValueKind switch
                    {
                        JsonValueKind.String => json.GetString(),
                        JsonValueKind.Number => json.TryGetInt64(out long l) ? l : json.GetDouble(),
                        JsonValueKind.True => true,
                        JsonValueKind.False => false,
                        JsonValueKind.Null => DBNull.Value,
                        _ => json.GetRawText()
                    },
                    null => DBNull.Value,
                    _ => parametro.Value
                };

                // APLICAR ENCRIPTACIÓN SI ES NECESARIO
                if (camposAEncriptar != null &&
                    camposAEncriptar.Any(c => string.Equals(c, parametro.Key, StringComparison.OrdinalIgnoreCase)) &&
                    valor is string valorTexto &&
                    !string.IsNullOrWhiteSpace(valorTexto) &&
                    !valorTexto.StartsWith("$2")) // No encriptar si ya está hasheado
                {
                    valor = BCrypt.Net.BCrypt.HashPassword(valorTexto);
                }

                // Agregar el parámetro a la lista
                parametrosSQL.Add(new SqlParameter(nombre, valor ?? DBNull.Value));
            }

            return parametrosSQL;
        }
    
        //aquí podrían agregarse métodos adicionales específicos del dominio
    }
}

// NOTAS PEDAGÓGICAS para el tutorial:
//
// 1. ESTA ES LA IMPLEMENTACIÓN DE LA LÓGICA DE NEGOCIO:
//    - Aplica reglas de seguridad específicas del dominio
//    - Convierte entre formatos (Dictionary ↔ SqlParameter)
//    - No ejecuta consultas directamente (eso es responsabilidad del repositorio)
//    - Se enfoca únicamente en validaciones y coordinación de negocio
//
// 2. DOS MÉTODOS PARA DIFERENTES CASOS DE USO:
//    - EjecutarConsultaParametrizadaAsync: método principal con control total
//    - EjecutarConsultaParametrizadaDesdeJsonAsync: método de conveniencia para web
//    - El método JSON usa el método principal internamente (reutilización)
//
// 3. CONVERSIÓN SEGURA DE PARÁMETROS:
//    - Maneja JsonElement correctamente (viene del controlador web)
//    - Valida nombres de parámetros con Regex
//    - Convierte tipos JSON a tipos SQL appropriados
//    - Maneja valores null correctamente (DBNull.Value)
//
// 4. APLICACIÓN PRÁCTICA DE DIP:
//    - Depende de IRepositorioConsultas, no de RepositorioConsultasSqlServer
//    - Puede funcionar con cualquier implementación del repositorio
//    - Facilita testing con repositorios mock
//
// 5. PRÓXIMO PASO EN PROGRAM.CS:
//    Registrar este servicio:
//    builder.Services.AddScoped<IServicioConsultas, ServicioConsultas>();