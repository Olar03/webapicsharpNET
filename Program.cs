// Program.cs - Actualizado con nueva estructura de Abstracciones

// Importa tipos básicos como TimeSpan para configurar tiempos.
using System;

// Importa la API para construir la aplicación web y el pipeline HTTP.
using Microsoft.AspNetCore.Builder;

// Importa la API para registrar servicios en el contenedor de inyección de dependencias (DIP).
using Microsoft.Extensions.DependencyInjection;

// Importa utilidades para detectar el entorno (Desarrollo, Producción, etc.).
using Microsoft.Extensions.Hosting;

// Crea el "builder": punto de inicio para configurar servicios y la aplicación.
var builder = WebApplication.CreateBuilder(args);

// ---------------------------------------------------------
// CONFIGURACIÓN (OCP: se puede extender sin tocar la lógica)
// ---------------------------------------------------------

// Agrega un archivo JSON opcional para configuraciones adicionales.
// Si "tablasprohibidas.json" existe, se carga; si no existe, no falla.
builder.Configuration.AddJsonFile(
    "tablasprohibidas.json",
    optional: true,
    reloadOnChange: true
);

// ---------------------------------------------------------
// SERVICIOS (SRP: solo registro, sin lógica de negocio aquí)
// ---------------------------------------------------------

// Agrega soporte para controladores. Los controladores viven en la carpeta "Controllers".
builder.Services.AddControllers();

//CORS (Cross-Origin Resource Sharing) Intercambio de recursos de origen cruzado
// permite que la API sea consumida desde otros dominios.
// Agrega CORS con una política genérica llamada "PermitirTodo".
// Permite consumir la API desde cualquier origen, método y encabezado.
// Más adelante se puede endurecer (DOMINIOS específicos) sin cambiar controladores (OCP).
builder.Services.AddCors(opts =>
{
    opts.AddPolicy("PermitirTodo", politica => politica
        .AllowAnyOrigin()
        .AllowAnyMethod()
        .AllowAnyHeader()
    );
});

// Agrega caché en memoria y sesión HTTP ligera (opcional).
// La sesión sirve para guardar datos pequeños por usuario durante un tiempo.
builder.Services.AddDistributedMemoryCache();
builder.Services.AddSession(opciones =>
{
    // Tiempo de inactividad permitido antes de expirar la sesión.
    opciones.IdleTimeout = TimeSpan.FromMinutes(30);

    // Marca la cookie de sesión como accesible solo por HTTP (más seguro).
    opciones.Cookie.HttpOnly = true;

    // Indica que la cookie es esencial para el funcionamiento (no depende de consentimientos).
    opciones.Cookie.IsEssential = true;
});

// Agrega servicios para exponer documentación Swagger/OpenAPI.
// Útil para probar endpoints manualmente mientras no existe frontend.
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// -----------------------------------------------------------------
// NOTA DIP: el registro de interfaces → implementaciones irá aquí.
// Ejemplos (se dejan comentados hasta el siguiente paso):
//
// builder.Services.AddScoped<IServicioCrud, ServicioCrud>();
// builder.Services.AddScoped<IRepositorioLecturaTabla, RepositorioLecturaSql>();
// builder.Services.AddSingleton<IValidadorIdentificadorSql, ValidadorIdentificadorSql>();
// builder.Services.AddSingleton<IPoliticaTablasProhibidas, PoliticaTablasProhibidas>();
// -----------------------------------------------------------------

// REGISTRO DE SERVICIO CRUD (DIP): interfaz → implementación (una instancia por request)
builder.Services.AddScoped<webapicsharp.Servicios.Abstracciones.IServicioCrud,
                           webapicsharp.Servicios.ServicioCrud>();

// REGISTRO DEL PROVEEDOR DE CONEXIÓN (DIP):
// Cuando se solicite IProveedorConexion, el contenedor entregará ProveedorConexion.
// NOTA: IProveedorConexion ahora está en Servicios.Abstracciones
builder.Services.AddSingleton<webapicsharp.Servicios.Abstracciones.IProveedorConexion,
                              webapicsharp.Servicios.Conexion.ProveedorConexion>();

// REGISTRO AUTOMÁTICO DEL REPOSITORIO SEGÚN DatabaseProvider (DIP + OCP)
// La API genérica lee la configuración y usa el proveedor correcto automáticamente
var proveedorBD = builder.Configuration.GetValue<string>("DatabaseProvider") ?? "SqlServer";

// REGISTRO DE SERVICIO CONSULTAS (DIP): interfaz → implementación (una instancia por request)
builder.Services.AddScoped<webapicsharp.Servicios.Abstracciones.IServicioConsultas,
                           webapicsharp.Servicios.ServicioConsultas>();



switch (proveedorBD.ToLower())
{
    case "postgres":
        // Usar PostgreSQL cuando DatabaseProvider = "Postgres"
        builder.Services.AddScoped<webapicsharp.Repositorios.Abstracciones.IRepositorioLecturaTabla,
                                  webapicsharp.Repositorios.RepositorioLecturaPostgreSQL>();

        builder.Services.AddScoped<webapicsharp.Repositorios.Abstracciones.IRepositorioConsultas,
                                    webapicsharp.Repositorios.RepositorioConsultasSqlServer>();
        break;
    case "mariadb":
    case "mysql":
        // Usar MariaDB cuando DatabaseProvider = "MariaDB" o "MySQL"
        builder.Services.AddScoped<webapicsharp.Repositorios.Abstracciones.IRepositorioLecturaTabla,
                                  webapicsharp.Repositorios.RepositorioLecturaMariaDB>();

        builder.Services.AddScoped<webapicsharp.Repositorios.Abstracciones.IRepositorioConsultas,
                                    webapicsharp.Repositorios.RepositorioConsultasSqlServer>();
        break;
    case "sqlserver":
    case "sqlserverexpress":
    case "localdb":
    default:
        // Usar SQL Server para todos los demás casos (incluyendo el valor por defecto)
        builder.Services.AddScoped<webapicsharp.Repositorios.Abstracciones.IRepositorioLecturaTabla,
                                   webapicsharp.Repositorios.RepositorioLecturaSqlServer>();

        builder.Services.AddScoped<webapicsharp.Repositorios.Abstracciones.IRepositorioConsultas,
                               webapicsharp.Repositorios.RepositorioConsultasSqlServer>();


        break;
}


// Construye la aplicación con todo lo configurado arriba.
var app = builder.Build();

// ---------------------------------------------------------
// MIDDLEWARE (orden importa: se ejecuta de arriba hacia abajo)
// ---------------------------------------------------------

// En Desarrollo se muestran páginas de error detalladas para depurar.
if (app.Environment.IsDevelopment())
{
    app.UseDeveloperExceptionPage();
}

// Habilita Swagger siempre y expone la UI en la ruta /swagger.
// Esto no afecta la lógica de negocio y se puede quitar en Producción.
app.UseSwagger();
app.UseSwaggerUI(c =>
{
    // Indica dónde vive el documento OpenAPI.
    c.SwaggerEndpoint("/swagger/v1/swagger.json", "webapicsharp v1");

    // Define el prefijo de ruta. Con esto, la UI queda en /swagger.
    c.RoutePrefix = "swagger";
});

// Redirige HTTP a HTTPS para mejorar la seguridad, si el certificado está configurado.
app.UseHttpsRedirection();

// Aplica la política CORS definida como "PermitirTodo".
app.UseCors("PermitirTodo");

// Habilita la sesión HTTP (si no se usa, se puede quitar sin tocar controladores).
app.UseSession();

// Agrega el middleware de autorización (para cuando existan endpoints protegidos).
app.UseAuthorization();

// Mapea las rutas de los controladores. Sin controladores aún, la API expone solo Swagger.
app.MapControllers();

// Arranca la aplicación y queda escuchando solicitudes HTTP.
app.Run();

