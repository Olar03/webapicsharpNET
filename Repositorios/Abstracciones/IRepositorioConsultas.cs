// IRepositorioConsultas.cs — Interface que define el contrato para ejecutar consultas SQL parametrizadas
// Ubicación: Repositorios/Abstracciones/IRepositorioConsultas.cs
//
// Principios SOLID aplicados:
// - SRP: Esta interface solo define operaciones de acceso a datos para consultas SQL parametrizadas
// - DIP: Permite que los servicios dependan de esta abstracción, no de implementaciones concretas
// - OCP: Abierta para extensión (nuevas implementaciones) pero cerrada para modificación
// - ISP: Interface específica y enfocada, solo contiene métodos para consultas SQL parametrizadas

using System.Collections.Generic;   // Para List<>
using System.Threading.Tasks;       // Para async/await
using System.Data;                  // Para DataTable
using Microsoft.Data.SqlClient;     // Para SqlParameter

namespace webapicsharp.Repositorios.Abstracciones
{
    /// <summary>
    /// Contrato para repositorios que ejecutan consultas SQL parametrizadas arbitrarias.
    /// 
    /// Esta interface define el "QUÉ" se puede hacer con consultas SQL personalizadas,
    /// no el "CÓMO" se ejecutan. Se diferencia de IRepositorioLecturaTabla en que:
    /// 
    /// - IRepositorioLecturaTabla: SELECT * FROM tabla (operaciones estándar)
    /// - IRepositorioConsultas: Consultas SQL arbitrarias con parámetros
    /// 
    /// Casos de uso:
    /// - Reportes complejos con JOINs múltiples
    /// - Consultas analíticas con agregaciones  
    /// - Procedimientos almacenados con parámetros
    /// - Consultas dinámicas construidas en runtime
    /// 
    /// Beneficios de esta abstracción:
    /// - Permite intercambiar proveedores de BD sin cambiar código cliente
    /// - Facilita testing mediante mocks
    /// - Desacopla ejecución de consultas de lógica de negocio
    /// - Soporta múltiples implementaciones (SQL Server, PostgreSQL, etc.)
    /// </summary>
    public interface IRepositorioConsultas
    {
        /// <summary>
        /// Ejecuta una consulta SQL parametrizada y devuelve los resultados como DataTable.
        /// 
        /// Este método ejecuta consultas SQL arbitrarias con parámetros seguros
        /// y devuelve los resultados manteniendo información de esquema.
        /// 
        /// Comportamiento esperado:
        /// - Si la consulta tiene errores de sintaxis, lanzar excepción descriptiva
        /// - Si los parámetros no coinciden, lanzar excepción específica
        /// - Si no hay resultados, devolver DataTable vacío (no null)
        /// - Manejar tipos de datos SQL correctamente
        /// </summary>
        /// <param name="consultaSQL">
        /// Consulta SQL parametrizada a ejecutar. Debe usar placeholders @nombre.
        /// 
        /// Ejemplos:
        /// - "SELECT * FROM productos WHERE precio > @precio"
        /// - "SELECT p.*, c.nombre FROM productos p JOIN categorias c ON p.categoria_id = c.id WHERE p.activo = @activo"
        /// </param>
        /// <param name="parametros">
        /// Lista de parámetros SQL para la consulta.
        /// Si es null o vacía, ejecuta consulta sin parámetros.
        /// 
        /// Ejemplo:
        /// new List<SqlParameter> {
        ///     new SqlParameter("@precio", 99.99),
        ///     new SqlParameter("@activo", true)
        /// }
        /// </param>
        /// <returns>
        /// DataTable con los resultados de la consulta.
        /// - Mantiene información de esquema (tipos, metadatos)
        /// - Si no hay resultados: DataTable vacío, no null
        /// - Mapeo automático de tipos SQL → .NET
        /// </returns>
        /// <exception cref="ArgumentException">
        /// Se lanza cuando hay problemas con parámetros de entrada:
        /// - consultaSQL vacía o null
        /// - Parámetros con nombres inválidos
        /// </exception>
        /// <exception cref="InvalidOperationException">
        /// Se lanza cuando hay problemas en la ejecución:
        /// - Error de sintaxis SQL
        /// - Tabla o columna no existe
        /// - Problemas de conectividad
        /// - Timeout en la consulta
        /// </exception>
        Task<DataTable> EjecutarConsultaParametrizadaAsync(
            string consultaSQL,
            List<SqlParameter>? parametros
        );

        /// <summary>
        /// Valida que una consulta SQL sea ejecutable sin ejecutarla.
        /// 
        /// Útil para validaciones previas sin impacto en la BD.
        /// </summary>
        /// <param name="consultaSQL">Consulta SQL a validar</param>
        /// <param name="parametros">Parámetros a validar</param>
        /// <returns>
        /// Tupla con resultado:
        /// - bool esValida: true si es ejecutable
        /// - string? mensajeError: descripción del error si no es válida
        /// </returns>
        Task<(bool esValida, string? mensajeError)> ValidarConsultaAsync(
            string consultaSQL,
            List<SqlParameter>? parametros
        );
        /// <summary>
        /// Ejecuta un procedimiento almacenado con parámetros y devuelve los resultados.
        /// </summary>
        /// <param name="nombreSP">Nombre del procedimiento almacenado a ejecutar</param>
        /// <param name="parametros">Lista de parámetros SQL para el procedimiento</param>
        /// <returns>DataTable con los resultados del procedimiento almacenado</returns>
        Task<DataTable> EjecutarProcedimientoAlmacenadoAsync(
            string nombreSP,
            List<SqlParameter>? parametros
        );
        /// <summary>
        /// Obtiene el esquema real donde existe una tabla específica.
        /// </summary>
        /// <param name="nombreTabla">Nombre de la tabla a buscar</param>
        /// <param name="esquemaPredeterminado">Esquema donde buscar primero</param>
        /// <returns>Nombre del esquema donde existe la tabla, o null si no existe</returns>
        Task<string?> ObtenerEsquemaTablaAsync(string nombreTabla, string esquemaPredeterminado);

        /// <summary>
        /// Obtiene la estructura detallada de una tabla (columnas, tipos, restricciones).
        /// </summary>
        /// <param name="nombreTabla">Nombre de la tabla</param>
        /// <param name="esquema">Esquema de la tabla</param>
        /// <returns>DataTable con la estructura completa de la tabla</returns>
        Task<DataTable> ObtenerEstructuraTablaAsync(string nombreTabla, string esquema);

        /// <summary>
        /// Obtiene la estructura completa de la base de datos (tablas, columnas, relaciones).
        /// </summary>
        /// <param name="nombreBD">Nombre de la base de datos (opcional)</param>
        /// <returns>DataTable con la estructura jerárquica de la base de datos</returns>
        Task<DataTable> ObtenerEstructuraBaseDatosAsync(string? nombreBD);

//AQUÍ AGREGAR MÁS MÉTODOS SI ES NECESARIO

    }
}