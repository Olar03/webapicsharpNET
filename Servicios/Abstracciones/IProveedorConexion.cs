// IProveedorConexion.cs — Interface que define el contrato para obtener conexiones a base de datos
// Ubicación: Servicios/Abstracciones/IProveedorConexion.cs
//
// Principios SOLID aplicados:
// - SRP: Esta interface solo se encarga de definir operaciones relacionadas con conexiones
// - DIP: Permite que otras clases dependan de esta abstracción, no de implementaciones concretas
// - ISP: Interface específica y pequeña, solo métodos relacionados con conexiones
// - OCP: Abierta para extensión (nuevas implementaciones) pero cerrada para modificación

using System; // Para InvalidOperationException en documentación

namespace webapicsharp.Servicios.Abstracciones
{
    /// <summary>
    /// Contrato que define cómo obtener información de conexión a base de datos.
    /// 
    /// Esta interface es el "qué" (contrato), no el "cómo" (implementación).
    /// Permite que cualquier clase que necesite conexiones dependa de esta abstracción
    /// en lugar de depender de una clase concreta específica.
    /// 
    /// Beneficios:
    /// - Facilita testing (se pueden crear mocks de esta interface)
    /// - Permite intercambiar implementaciones sin cambiar código cliente
    /// - Desacopla la lógica de obtener conexiones de quienes las usan
    /// </summary>
    public interface IProveedorConexion
    {
        /// <summary>
        /// Obtiene el nombre del proveedor de base de datos actualmente configurado.
        /// 
        /// Este valor viene típicamente de appsettings.json en la clave "DatabaseProvider".
        /// 
        /// Valores esperados:
        /// - "SqlServer" para Microsoft SQL Server
        /// - "SqlServerEXPRESS" para SQL Server Express Edition
        /// - "LocalDb" para SQL Server LocalDB (desarrollo)
        /// - "Postgres" para PostgreSQL
        /// - "MariaDB" para MariaDB
        /// - "MySQL" para MySQL
        /// 
        /// Ejemplo de uso:
        /// var proveedor = proveedorConexion.ProveedorActual;
        /// if (proveedor == "Postgres") { /* lógica específica de PostgreSQL */ }
        /// </summary>
        /// <returns>
        /// Nombre del proveedor configurado. Si no está configurado, debería devolver 
        /// un valor por defecto como "SqlServer"
        /// </returns>
        string ProveedorActual { get; }

        /// <summary>
        /// Obtiene la cadena de conexión correspondiente al proveedor configurado.
        /// 
        /// Esta cadena contiene todos los parámetros necesarios para conectar a la base de datos:
        /// - Servidor/Host
        /// - Base de datos
        /// - Credenciales (usuario/contraseña) 
        /// - Parámetros adicionales (timeout, pooling, etc.)
        /// 
        /// La implementación debe:
        /// 1. Leer ProveedorActual 
        /// 2. Buscar en configuración la cadena correspondiente
        /// 3. Validar que la cadena existe
        /// 4. Devolver la cadena lista para usar
        /// 
        /// Ejemplo de cadenas típicas:
        /// - SQL Server: "Server=localhost;Database=MiDB;Trusted_Connection=true;"
        /// - PostgreSQL: "Host=localhost;Database=midb;Username=user;Password=pass;"
        /// - MariaDB: "Server=localhost;Database=midb;Uid=user;Pwd=pass;"
        /// </summary>
        /// <returns>
        /// Cadena de conexión completa lista para usar con ADO.NET o Entity Framework
        /// </returns>
        /// <exception cref="InvalidOperationException">
        /// Se lanza cuando:
        /// - No existe configuración para el proveedor actual
        /// - La cadena de conexión está vacía o es null
        /// - Hay un error en la configuración
        /// 
        /// El mensaje de error debe ser claro para facilitar debugging:
        /// "No se encontró la cadena de conexión para el proveedor 'SqlServer'. 
        ///  Verificar 'ConnectionStrings' y 'DatabaseProvider' en appsettings.json."
        /// </exception>
        string ObtenerCadenaConexion();
    }
}

// NOTAS PEDAGÓGICAS para el tutorial:
//
// 1. ESTA ES UNA ABSTRACCIÓN (Interface):
//    - Define "QUÉ" operaciones se pueden hacer
//    - NO define "CÓMO" se hacen (eso va en la implementación)
//    - Es un contrato que promete cierta funcionalidad
//
// 2. ¿POR QUÉ USAR INTERFACE?
//    - DIP: Otras clases dependen de esta abstracción, no de implementación concreta
//    - Testing: Se puede crear un "fake" IProveedorConexion para pruebas
//    - Flexibilidad: Se puede cambiar implementación sin afectar código cliente
//
// 3. ¿QUÉ VIENE DESPUÉS?
//    - Crear ProveedorConexion.cs que implemente esta interface
//    - Registrar en Program.cs: builder.Services.AddSingleton<IProveedorConexion, ProveedorConexion>()
//    - Usar en repositorios: constructor(IProveedorConexion proveedor)
//
// 4. EJEMPLO DE USO FUTURO:
//    public class RepositorioLecturaSqlServer
//    {
//        private readonly IProveedorConexion _proveedor;
//        
//        public RepositorioLecturaSqlServer(IProveedorConexion proveedor)  // ← Recibe la abstracción
//        {
//            _proveedor = proveedor;
//        }
//        
//        public async Task<List<Dictionary<string, object>>> ListarAsync(string tabla)
//        {
//            var cadena = _proveedor.ObtenerCadenaConexion();  // ← Usa la abstracción
//            // ... usar la cadena para conectar
//        }
//    }
//
// 5. RELACIÓN CON PROGRAM.CS:
//    Después de crear esta interface y su implementación, se podrá descomentar:
//    builder.Services.AddSingleton<webapicsharp.Servicios.Abstracciones.IProveedorConexion,
//                                  webapicsharp.Servicios.Conexion.ProveedorConexion>();
