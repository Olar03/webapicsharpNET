// ProveedorConexion.cs — Implementación que lee configuración desde appsettings.json
// Ubicación: Servicios/Conexion/ProveedorConexion.cs
//
// Principios SOLID aplicados:
// - SRP: Esta clase solo sabe "leer configuración y entregar una cadena de conexión"
// - DIP: Implementa IProveedorConexion, permitiendo que otras clases dependan de la abstracción
// - OCP: Si mañana se agrega MySQL/Oracle, se extiende sin tocar el resto del sistema

using Microsoft.Extensions.Configuration;     // Permite leer appsettings.*.json
using System;                                  // Para InvalidOperationException
using webapicsharp.Servicios.Abstracciones;   // Para IProveedorConexion

namespace webapicsharp.Servicios.Conexion
{
   /// <summary>
   /// Implementación concreta que lee "DatabaseProvider" y "ConnectionStrings" desde IConfiguration.
   /// 
   /// Esta clase encapsula toda la lógica específica de cómo obtener configuraciones:
   /// - Lee desde archivos appsettings.json
   /// - Maneja valores por defecto
   /// - Proporciona mensajes de error claros
   /// - Valida que las configuraciones existan
   /// 
   /// Responsabilidad única: conectar la configuración JSON con los repositorios que necesitan
   /// cadenas de conexión, sin que estos sepan de dónde vienen.
   /// </summary>
   public class ProveedorConexion : IProveedorConexion
   {
       // Campo privado que mantiene referencia a la configuración inyectada desde Program.cs
       // Aplica DIP: dependemos de IConfiguration (abstracción), no de implementación específica
       private readonly IConfiguration _configuracion;

       /// <summary>
       /// Constructor que recibe la configuración por inyección de dependencias.
       /// 
       /// El patrón de inyección de dependencias funciona así:
       /// 1. Program.cs registra: AddSingleton<IProveedorConexion, ProveedorConexion>()
       /// 2. Cuando alguien solicita IProveedorConexion, el contenedor crea ProveedorConexion
       /// 3. El contenedor ve que ProveedorConexion necesita IConfiguration
       /// 4. El contenedor inyecta automáticamente IConfiguration en este constructor
       /// 
       /// Beneficios de este patrón:
       /// - No necesitamos crear manualmente las dependencias
       /// - Facilita testing: podemos inyectar configuración mock
       /// - Centraliza la creación de objetos en Program.cs
       /// - Permite cambiar implementaciones sin tocar este código
       /// </summary>
       /// <param name="configuracion">
       /// Configuración de la aplicación que viene automáticamente desde Program.cs.
       /// Contiene todos los datos de appsettings.json y appsettings.Development.json.
       /// El contenedor DI de ASP.NET Core gestiona esta inyección automáticamente.
       /// </param>
       /// <exception cref="ArgumentNullException">
       /// Se lanza si configuracion es null, lo cual indicaría un problema en la configuración
       /// de inyección de dependencias en Program.cs o un error en el registro de servicios.
       /// </exception>
       public ProveedorConexion(IConfiguration configuracion)
       {
           // Validación defensiva: asegurar que la inyección de dependencias funcionó correctamente
           // Si es null, significa que hay un problema grave en Program.cs o en el contenedor DI
           // Esta validación evita NullReferenceException más adelante en métodos de la clase
           _configuracion = configuracion ?? throw new ArgumentNullException(
               nameof(configuracion), 
               "IConfiguration no puede ser null. Verificar configuración de inyección de dependencias en Program.cs."
           );
       }

       /// <summary>
       /// Implementa IProveedorConexion.ProveedorActual
       /// 
       /// Lee el valor de "DatabaseProvider" desde appsettings.json. 
       /// Si no existe o está vacío, por defecto usa "SqlServer" para facilitar desarrollo.
       /// 
       /// Configuración típica en appsettings.json:
       /// {
       ///   "DatabaseProvider": "Postgres",
       ///   "ConnectionStrings": {
       ///     "Postgres": "Host=localhost;Database=midb;Username=user;Password=pass;"
       ///   }
       /// }
       /// 
       /// Esta propiedad permite que el mismo código funcione con diferentes bases de datos
       /// simplemente cambiando configuración, sin recompilar (aplicando OCP).
       /// </summary>
       public string ProveedorActual
       {
           get
           {
               // _configuracion.GetValue<string>() es una forma segura de leer configuración
               // Ventajas sobre otras formas de leer configuración:
               // - Si la clave no existe, devuelve null (no lanza excepción)
               // - Si existe pero está vacía, devuelve string vacío
               // - Maneja conversiones de tipos automáticamente
               var valor = _configuracion.GetValue<string>("DatabaseProvider");
               
               // Si es null o vacío, devolver "SqlServer" como valor seguro para desarrollo
               // Trim() elimina espacios en blanco que podrían causar problemas en el switch de Program.cs
               // El valor por defecto facilita que el tutorial funcione sin configuración inicial
               return string.IsNullOrWhiteSpace(valor) ? "SqlServer" : valor.Trim();
           }
       }

       /// <summary>
       /// Implementa IProveedorConexion.ObtenerCadenaConexion()
       /// 
       /// Entrega la cadena de conexión correspondiente al proveedor actual.
       /// Esta es la función principal de la clase: traducir configuración a cadenas de conexión.
       /// 
       /// El proceso interno es:
       /// 1. Obtiene ProveedorActual (ej: "Postgres")
       /// 2. Busca en la sección ConnectionStrings una entrada con ese nombre exacto
       /// 3. Valida que la cadena existe y no está vacía
       /// 4. Devuelve la cadena lista para usar con ADO.NET
       /// 
       /// ¿Por qué este método es importante?
       /// - Centraliza la lógica de obtener conexiones
       /// - Proporciona mensajes de error claros para facilitar debugging
       /// - Permite cambiar de base de datos sin tocar código de repositorios
       /// - Valida configuración al momento de usarla, no al inicio de la app
       /// </summary>
       /// <returns>
       /// Cadena de conexión completa lista para usar con ADO.NET y proveedores específicos.
       /// 
       /// Ejemplos de cadenas que puede devolver:
       /// - SQL Server: "Server=localhost;Database=MiDB;Integrated Security=true;TrustServerCertificate=true;"
       /// - PostgreSQL: "Host=localhost;Port=5432;Database=midb;Username=user;Password=pass;"
       /// - MariaDB: "Server=localhost;Port=3306;Database=midb;Uid=user;Pwd=pass;"
       /// 
       /// Estas cadenas se usan directamente con:
       /// - new SqlConnection(cadena) para SQL Server
       /// - new NpgsqlConnection(cadena) para PostgreSQL  
       /// - new MySqlConnection(cadena) para MariaDB
       /// </returns>
       /// <exception cref="InvalidOperationException">
       /// Se lanza cuando no se encuentra la configuración necesaria.
       /// El mensaje incluye información específica para facilitar debugging:
       /// - Qué proveedor se buscó
       /// - Dónde debería estar configurado
       /// - Qué archivos verificar
       /// 
       /// Esta excepción ayuda al desarrollador a identificar rápidamente problemas de configuración
       /// en lugar de obtener errores genéricos de conexión más adelante.
       /// </exception>
       public string ObtenerCadenaConexion()
       {
           // GetConnectionString() es un método específico de IConfiguration 
           // que busca automáticamente en la sección "ConnectionStrings" del archivo de configuración
           // Es equivalente a: _configuracion.GetSection("ConnectionStrings")[ProveedorActual]
           // pero más limpio y expresivo en el código
           string? cadena = _configuracion.GetConnectionString(ProveedorActual);

           // Validación crítica: si no existe la cadena, lanzar excepción con mensaje detallado
           // Esta validación temprana evita errores más crípticos al intentar conectar a la BD
           // El mensaje de error incluye toda la información necesaria para resolver el problema
           if (string.IsNullOrWhiteSpace(cadena))
           {
               throw new InvalidOperationException(
                   $"No se encontró la cadena de conexión para el proveedor '{ProveedorActual}'. " +
                   $"Verificar que existe 'ConnectionStrings:{ProveedorActual}' en appsettings.json " +
                   $"y que 'DatabaseProvider' esté configurado correctamente. " +
                   $"Archivos a revisar: appsettings.json, appsettings.Development.json"
               );
           }

           // Devolver la cadena lista para ser usada por repositorios
           // Los repositorios usarán esta cadena con el proveedor específico:
           // SqlConnection, NpgsqlConnection, MySqlConnection, etc.
           // La cadena contiene todos los parámetros: servidor, base de datos, credenciales, opciones
           return cadena;
       }
   }
}

// NOTAS PEDAGÓGICAS para el tutorial:
//
// 1. ESTA ES LA IMPLEMENTACIÓN CONCRETA:
//    - Implementa la interface IProveedorConexion definida en Abstracciones/
//    - Define "CÓMO" se obtienen las conexiones (leyendo appsettings.json)
//    - Contiene lógica específica de Microsoft.Extensions.Configuration
//    - Es intercambiable: mañana podrías crear ProveedorConexionDesdeVariablesEntorno
//
// 2. SEPARACIÓN DE RESPONSABILIDADES (SRP aplicado):
//    - IProveedorConexion (en Abstracciones/) define QUÉ se puede hacer
//    - ProveedorConexion (aquí) define CÓMO se hace específicamente
//    - Esta separación permite cambiar implementación sin afectar código cliente
//    - Los repositorios no saben si la configuración viene de JSON, XML, base de datos, etc.
//
// 3. INYECCIÓN DE DEPENDENCIAS (DIP aplicado):
//    - Constructor recibe IConfiguration automáticamente desde Program.cs
//    - No necesita saber de dónde viene la configuración (archivos, variables, secrets)
//    - Facilita testing: se puede inyectar IConfiguration mock con datos de prueba
//    - Centraliza la creación de objetos en el contenedor DI
//
// 4. MANEJO DE ERRORES DEFENSIVO:
//    - Validación defensiva en constructor (evita problemas de DI)
//    - Mensajes de error claros y específicos (facilitan debugging)
//    - Excepciones con información útil para resolver problemas
//    - Validación temprana (falla rápido si hay problemas de configuración)
//
// 5. CONFIGURACIÓN ESPERADA:
//    En appsettings.json debe existir:
//    {
//      "DatabaseProvider": "SqlServer",    // o "Postgres", "MariaDB", etc.
//      "ConnectionStrings": {
//        "SqlServer": "Server=localhost;Database=MiDB;Integrated Security=true;TrustServerCertificate=true;",
//        "Postgres": "Host=localhost;Database=midb;Username=user;Password=pass;Pooling=true;",
//        "MariaDB": "Server=localhost;Database=midb;Uid=user;Pwd=pass;"
//      }
//    }
//
// 6. PRÓXIMO PASO EN EL TUTORIAL:
//    Después de crear este archivo, se puede descomentar en Program.cs:
//    builder.Services.AddSingleton<webapicsharp.Servicios.Abstracciones.IProveedorConexion,
//                                  webapicsharp.Servicios.Conexion.ProveedorConexion>();
//
// 7. ¿POR QUÉ AddSingleton Y NO AddScoped?
//    - Singleton: Una instancia para toda la aplicación (se comparte entre requests)
//    - La configuración no cambia durante la ejecución, entonces es seguro compartirla
//    - Mejor rendimiento: no se crea una instancia nueva por cada request HTTP
//    - Los repositorios que dependan de esto recibirán siempre la misma instancia
//
// 8. TESTING DE ESTA CLASE:
//    Para probar esta clase, puedes crear un IConfiguration mock:
//    var config = new ConfigurationBuilder()
//        .AddInMemoryCollection(new[] {
//            new KeyValuePair<string, string>("DatabaseProvider", "SqlServer"),
//            new KeyValuePair<string, string>("ConnectionStrings:SqlServer", "test connection")
//        }).Build();
//    var proveedor = new ProveedorConexion(config);
//    Assert.Equal("SqlServer", proveedor.ProveedorActual);
