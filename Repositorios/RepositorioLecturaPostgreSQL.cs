
using Npgsql;                              // Para conectar y ejecutar comandos en PostgreSQL
using System;                                          // Para excepciones y tipos básicos del sistema
using System.Collections.Generic;                      // Para List<> y Dictionary<> genéricos
using System.Threading.Tasks;                          // Para programación asíncrona con async/await                  
using webapicsharp.Repositorios.Abstracciones;        // Para implementar la interfaz IRepositorioLecturaTabla
using webapicsharp.Servicios.Abstracciones;           // Para usar IProveedorConexion y obtener cadenas de conexión
using webapicsharp.Servicios.Utilidades;
using System.Linq;                                   // Para operaciones LINQ como Select y Where


namespace webapicsharp.Repositorios
{
    /// <summary>
    /// Implementa IRepositorioLecturaTabla para leer datos de una tabla en PostgreSQL.
    /// </summary>
    public class RepositorioLecturaPostgreSQL : IRepositorioLecturaTabla
    {
        private readonly IProveedorConexion _proveedorConexion;

        /// <summary>
        /// Constructor que recibe el proveedor de conexión mediante inyección de dependencias.
        /// </summary>
        /// <param name="proveedorConexion">Proveedor de conexión para obtener cadenas de conexión.</param>
        public RepositorioLecturaPostgreSQL(IProveedorConexion proveedorConexion)
        {
            _proveedorConexion = proveedorConexion ?? throw new ArgumentNullException(nameof(proveedorConexion), "El proveedor de conexión no puede ser nulo."
            );
        }

        /// <summary>
        /// Lee todos los registros de la tabla especificada.
        /// </summary>
        /// <param name="nombreTabla">Nombre de la tabla a leer.</param>
        /// <returns>Lista de diccionarios representando los registros.</returns>
        public async Task<IReadOnlyList<Dictionary<string, object?>>> ObtenerFilasAsync(
            string nombreTabla,    // Nombre de la tabla (requerido)
            string? esquema,       // Esquema de postgre (opcional, por defecto "public")
            int? limite
        )
        {
            if (string.IsNullOrWhiteSpace(nombreTabla))
                throw new ArgumentException("El nombre de la tabla no puede estar vacío.", nameof(nombreTabla));

            //Normalización del parametro esquema
            string esquemaNormalizado = string.IsNullOrWhiteSpace(esquema) ? "public" : esquema.Trim();

            int limiteNormalizado = limite ?? 100;

            //Contrucción de la consulta SQL
            string PtsConsulta = $"SELECT * FROM \"{esquemaNormalizado}\".\"{nombreTabla}\" LIMIT @Limite";
            


            var resultados = new List<Dictionary<string, object?>>();

            try
            {
                //Conexión a la base de datos PostgreSQL
                using var conexion = new NpgsqlConnection(_proveedorConexion.ObtenerCadenaConexion());
                await conexion.OpenAsync();

                using var comando = new NpgsqlCommand(PtsConsulta, conexion);
                comando.Parameters.AddWithValue("@Limite", limiteNormalizado);
                using var lector = await comando.ExecuteReaderAsync();

                //Procesamiento de resultados
                while (await lector.ReadAsync())
                {
                    var fila = new Dictionary<string, object?>();
                    for (int indiceColumna = 0; indiceColumna < lector.FieldCount; indiceColumna++)
                    {

                        string nombreColumna = lector.GetName(indiceColumna);// Obtener el nombre de la columna
                                                                             // Manejo de valores nulos
                        object? valorColumna = lector.IsDBNull(indiceColumna) ? null : lector.GetValue(indiceColumna);


                        fila[nombreColumna] = valorColumna; // Agregar el par nombre-valor al diccionario de la fila


                    }
                    resultados.Add(fila); // Agregar la fila completa a la lista de resultados
                }
            }
            catch (NpgsqlException ex)
            {
                // Manejo de errores específicos de PostgreSQL
                throw ManejadorErroresPostgres.Procesar(ex, "ObtenerFilasAsync", nombreTabla);
            }            

            return resultados;
        }

        public async Task<IReadOnlyList<Dictionary<string, object?>>> ObtenerPorClaveAsync(
            string nombreTabla,     // Tabla objetivo para la consulta filtrada
            string? esquema,        // Esquema opcional (null = usar "public" por defecto)
            string nombreClave,     // Columna para aplicar filtro WHERE
            string valor           // Valor específico a buscar (se usa como parámetro)
                )
        {
            // Validaciones iniciales de parámetros
            if (string.IsNullOrWhiteSpace(nombreTabla))
                throw new ArgumentException("El nombre de la tabla no puede estar vacío.", nameof(nombreTabla));
            if (string.IsNullOrWhiteSpace(nombreClave))
                throw new ArgumentException("El nombre de la clave no puede estar vacío.", nameof(nombreClave));
            if (string.IsNullOrWhiteSpace(valor))
                throw new ArgumentException("El valor de la clave no puede estar vacío.", nameof(valor));

            //Normalización del parametro esquema
            string esquemaNormalizado = string.IsNullOrWhiteSpace(esquema) ? "public" : esquema.Trim();

            // Construcción de la consulta SQL con parámetro
            string PtsConsulta = $"SELECT * FROM \"{esquemaNormalizado}\".\"{nombreTabla}\" WHERE \"{nombreClave}\" = @ValorClave";

            //lista para almacenar los resultados
            var resultados = new List<Dictionary<string, object?>>();

            try
            {
                // Conexión a la base de datos PostgreSQL
                string cadenaConexion = _proveedorConexion.ObtenerCadenaConexion();
                using var conexion = new NpgsqlConnection(cadenaConexion);
                await conexion.OpenAsync();

                using var comando = new NpgsqlCommand(PtsConsulta, conexion); // Crear el comando SQL
                // Agregar el parámetro para evitar inyección SQL
                comando.Parameters.AddWithValue("@ValorClave", valor);

                using var lector = await comando.ExecuteReaderAsync();

                // Procesamiento de resultados
                while (await lector.ReadAsync())
                {
                    var fila = new Dictionary<string, object?>(); //Crear un diccionario para la fila actual
                    for (int indiceColumna = 0; indiceColumna < lector.FieldCount; indiceColumna++)// Iterar sobre todas las columnas de la fila
                    {
                        string nombreColumna = lector.GetName(indiceColumna); // Obtener el nombre de la columna
                        object? valorColumna = lector.IsDBNull(indiceColumna) ? null : lector.GetValue(indiceColumna); // Obtener el valor de la columna, manejando valores nulos

                        fila[nombreColumna] = valorColumna; // Agregar el par nombre-valor al diccionario de la fila
                    }
                    resultados.Add(fila); // Agregar la fila completa a la lista de resultados
                }
            }
            catch (NpgsqlException ex)
            {
                // Manejo de errores específicos de PostgreSQL
                throw ManejadorErroresPostgres.Procesar(ex, "ObtenerPorClaveAsync", nombreTabla);
            }
           
            return resultados;


        }

        public async Task<bool> CrearAsync(
            string nombreTabla,
            string? esquema,
            Dictionary<string, object?> datos,
            string? camposEncriptar = null
        )
        {
            //=============================================================
            // Validaciones iniciales de parámetros
            //=============================================================

            if (string.IsNullOrWhiteSpace(nombreTabla))
                throw new ArgumentException("El nombre de la tabla no puede estar vacío.", nameof(nombreTabla));

            if (datos == null || !datos.Any())
                throw new ArgumentException("Los datos para insertar no pueden estar vacíos.", nameof(datos));

            //Normalización del parametro esquema
            string esquemaNormalizado = string.IsNullOrWhiteSpace(esquema) ? "public" : esquema.Trim();

            var datosFinales = new Dictionary<string, object?>(datos);

            //=============================================================
            // encriptación de campos si es necesario
            //=============================================================
            if (!string.IsNullOrWhiteSpace(camposEncriptar))
            {
                var campos = camposEncriptar.Split(',')
                    .Select(c => c.Trim())
                    .Where(c => !string.IsNullOrEmpty(c))
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);

                foreach (var campo in campos)
                {
                    if (datosFinales.ContainsKey(campo) && datosFinales[campo] != null)
                    {
                        string valorOriginal = datosFinales[campo]?.ToString() ?? "";

                        datosFinales[campo] = webapicsharp.Servicios.Utilidades.EncriptacionBCrypt.Encriptar(valorOriginal);
                    }
                }
            }

            //=============================================================
            // Construcción de la consulta SQL y parametros
            //=============================================================

            //Columnas y parámetros
            var columnas = string.Join(", ", datosFinales.Keys.Select(k => $"\"{k}\""));
            var parametros = string.Join(", ", datosFinales.Keys.Select(k => $"@{k}"));

            string PtsConsulta = $"INSERT INTO \"{esquemaNormalizado}\".\"{nombreTabla}\" ({columnas}) VALUES ({parametros})";

            try
            {
                //Conexión a la base de datos PostgreSQL
                string cadenaConexion = _proveedorConexion.ObtenerCadenaConexion();
                using var conexion = new NpgsqlConnection(cadenaConexion);
                await conexion.OpenAsync();

                using var comando = new NpgsqlCommand(PtsConsulta, conexion);

                //Agregar parámetros al comando
                foreach (var kvp in datosFinales)
                {
                    comando.Parameters.AddWithValue($"@{kvp.Key}", kvp.Value ?? DBNull.Value);
                }

                int filasAfectadas = await comando.ExecuteNonQueryAsync();
                return filasAfectadas > 0;


            }
            catch (NpgsqlException ex)
            {
                // Manejo de errores específicos de PostgreSQL
                throw ManejadorErroresPostgres.Procesar(ex, "CrearAsync", nombreTabla);
            }
           

        }


        public async Task<int> ActualizarAsync(
            string nombreTabla,
            string? esquema,
            string nombreClave,
            string valorClave,
            Dictionary<string, object?> datos,
            string? camposEncriptar = null
)
        {
            //=============================================================
            // Validaciones iniciales de parámetros 
            //=============================================================

            if (string.IsNullOrWhiteSpace(nombreTabla))
                throw new ArgumentException("El nombre de la tabla no puede estar vacío.", nameof(nombreTabla));
            if (string.IsNullOrWhiteSpace(nombreClave))
                throw new ArgumentException("El nombre de la clave no puede estar vacío.", nameof(nombreClave
            ));
            if (string.IsNullOrWhiteSpace(valorClave))
                throw new ArgumentException("El valor de la clave no puede estar vacío.", nameof(valorClave));
            if (datos == null || !datos.Any())
                throw new ArgumentException("Los datos para actualizar no pueden estar vacíos.", nameof(datos));

            //Normalización del parametro esquema
            string esquemaNormalizado = string.IsNullOrWhiteSpace(esquema) ? "public" : esquema.Trim();

            var datosFinales = new Dictionary<string, object?>(datos);

            //=============================================================
            // encriptación de campos si es necesario
            //=============================================================

            if (!string.IsNullOrWhiteSpace(camposEncriptar))
            {
                var campos = camposEncriptar.Split(',')
                    .Select(c => c.Trim())
                    .Where(c => !string.IsNullOrEmpty(c))
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);

                foreach (var campo in campos)
                {
                    if (datosFinales.ContainsKey(campo) && datosFinales[campo] != null)
                    {
                        string valorOriginal = datosFinales[campo]?.ToString() ?? "";

                        datosFinales[campo] = webapicsharp.Servicios.Utilidades.EncriptacionBCrypt.Encriptar(valorOriginal);
                    }
                }
            }

            //=============================================================
            // Construcción de la consulta SQL y parametros
            //============================================================= 

            //Contrucción de la cláusula SET
            var clausulaSet = string.Join(", ", datosFinales.Keys.Select(k => $"\"{k}\" = @{k}"));

            //construcción de la consulta SQL
            string PtsConsulta = $"UPDATE \"{esquemaNormalizado}\".\"{nombreTabla}\" SET {clausulaSet} WHERE \"{nombreClave}\" = @ValorClave";

            try
            {
                // Conexión a la base de datos PostgreSQL
                string cadenaConexion = _proveedorConexion.ObtenerCadenaConexion();
                using var conexion = new NpgsqlConnection(cadenaConexion);
                await conexion.OpenAsync();

                using var comando = new NpgsqlCommand(PtsConsulta, conexion);

                //Agregar parámetros al comando}
                foreach (var kvp in datosFinales)
                {
                    comando.Parameters.AddWithValue($"@{kvp.Key}", kvp.Value ?? DBNull.Value);
                }

                //agregar el parámetrs de la clausula WHERE
                comando.Parameters.AddWithValue("@ValorClave", valorClave);

                //Ejecutar el comando
                int filasAfectadas = await comando.ExecuteNonQueryAsync();
                return filasAfectadas;
            }
            catch (NpgsqlException ex)
            {
                // Manejo de errores específicos de PostgreSQL
                throw ManejadorErroresPostgres.Procesar(ex, "ActualizarAsync", nombreTabla);
            }
            



        }

        public async Task<int> EliminarAsync(
            string nombreTabla,
            string? esquema,
            string nombreClave,
            string valorClave
        )
        {
            //validaciones iniciales de parámetros 
            if (string.IsNullOrWhiteSpace(nombreTabla))
                throw new ArgumentException("El nombre de la tabla no puede estar vacío.", nameof(nombreTabla));
            if (string.IsNullOrWhiteSpace(nombreClave))
                throw new ArgumentException("El nombre de la clave no puede estar vacío.", nameof(nombreClave));
            if (string.IsNullOrWhiteSpace(valorClave))
                throw new ArgumentException("El valor de la clave no puede estar vacío.", nameof(valorClave));

            //Normalización del parametro esquema
            string esquemaNormalizado = string.IsNullOrWhiteSpace(esquema) ? "public" : esquema.Trim();

            //Construcción de la consulta SQL
            string PtsConsulta = $"DELETE FROM \"{esquemaNormalizado}\".\"{nombreTabla}\" WHERE \"{nombreClave}\" = @ValorClave";

            try
            {
                // Conexión a la base de datos PostgreSQL
                string cadenaConexion = _proveedorConexion.ObtenerCadenaConexion();
                using var conexion = new NpgsqlConnection(cadenaConexion);
                await conexion.OpenAsync();

                using var comando = new NpgsqlCommand(PtsConsulta, conexion);

                //Agregar el parámetrs de la clausula WHERE
                comando.Parameters.AddWithValue("@ValorClave", valorClave);

                //Ejecutar el comando
                int filasAfectadas = await comando.ExecuteNonQueryAsync();
                return filasAfectadas;
            }
            catch (NpgsqlException ex)
            {
                // Manejo de errores específicos de PostgreSQL
                throw ManejadorErroresPostgres.Procesar(ex, "EliminarAsync", nombreTabla);
            }
            
        }

        public async Task<string?> ObtenerHashContrasenaAsync(
            string nombreTabla,
            string? esquema,
            string campoUsuario,
            string campoContrasena,
            string valorUsuario
)
        {
            //validaciones iniciales de parámetros
            if (string.IsNullOrWhiteSpace(nombreTabla))
                throw new ArgumentException("El nombre de la tabla no puede estar vacío.", nameof(nombreTabla));
            if (string.IsNullOrWhiteSpace(campoUsuario))
                throw new ArgumentException("El nombre del campo usuario no puede estar vacío.", nameof(campoUsuario));
            if (string.IsNullOrWhiteSpace(campoContrasena))
                throw new ArgumentException("El nombre del campo contraseña no puede estar vacío.", nameof(campoContrasena));
            if (string.IsNullOrWhiteSpace(valorUsuario))
                throw new ArgumentException("El valor del usuario no puede estar vacío.", nameof(valorUsuario));

            //Normalización del parametro esquema
            string esquemaNormalizado = string.IsNullOrWhiteSpace(esquema) ? "public" : esquema.Trim();

            //Construcción de la consulta SQL
            string PtsConsulta = $"SELECT \"{campoContrasena}\" FROM \"{esquemaNormalizado}\".\"{nombreTabla}\" WHERE \"{campoUsuario}\" = @ValorUsuario";

            try
            {
                //Conexión a la base de datos PostgreSQL
                using var conexion = new NpgsqlConnection(_proveedorConexion.ObtenerCadenaConexion());
                await conexion.OpenAsync();

                using var comando = new NpgsqlCommand(PtsConsulta, conexion);
                comando.Parameters.AddWithValue("@ValorUsuario", valorUsuario);

                var resultado = await comando.ExecuteScalarAsync();

                return resultado?.ToString();
            }
            catch (NpgsqlException ex)
            {
                // Manejo de errores específicos de PostgreSQL
                throw ManejadorErroresPostgres.Procesar(ex, "ObtenerHashContrasenaAsync", nombreTabla);
            }
            
        }




    }
}