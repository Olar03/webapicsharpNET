// EncriptacionBCrypt.cs — Clase de utilidad para manejo de encriptación BCrypt
// Ubicación: Servicios/Utilidades/EncriptacionBCrypt.cs
//
// Principios SOLID aplicados:
// - SRP: Esta clase solo se encarga de operaciones de encriptación/verificación
// - OCP: Abierta para extensión (nuevos métodos de hash) cerrada para modificación

using BCrypt.Net;
using System;

namespace webapicsharp.Servicios.Utilidades
{
    /// <summary>
    /// Clase estática para operaciones de encriptación usando BCrypt.
    /// 
    /// BCrypt es específicamente diseñado para hashear contraseñas y datos sensibles
    /// que requieren verificación pero no recuperación del valor original.
    /// 
    /// Características de BCrypt implementadas:
    /// - Salt automático único por cada hash
    /// - Costo computacional ajustable para resistir ataques de fuerza bruta
    /// - Hashing irreversible para máxima seguridad
    /// - Verificación segura sin exponer el hash original
    /// </summary>
    public static class EncriptacionBCrypt
    {
        // Costo por defecto de BCrypt (12 es un balance entre seguridad y rendimiento)
        // Valores más altos = más seguro pero más lento
        // En hardware moderno: 10=~10ms, 12=~150ms, 15=~5s
        private const int CostoPorDefecto = 12;

        /// <summary>
        /// Encripta (hashea) un valor usando BCrypt con salt automático.
        /// 
        /// Proceso interno de BCrypt:
        /// 1. Genera un salt aleatorio único
        /// 2. Combina el valor con el salt
        /// 3. Aplica el algoritmo de hashing BCrypt con el costo especificado
        /// 4. Devuelve un hash que incluye salt, costo y hash en formato estándar
        /// 
        /// El resultado es un string de 60 caracteres que contiene toda la información
        /// necesaria para verificar el valor posteriormente.
        /// </summary>
        /// <param name="valorOriginal">Valor a encriptar (contraseña, PIN, etc.)</param>
        /// <param name="costo">Costo computacional del hashing (por defecto 12)</param>
        /// <returns>
        /// Hash BCrypt de 60 caracteres que incluye:
        /// - Identificador del algoritmo
        /// - Costo utilizado
        /// - Salt generado
        /// - Hash resultante
        /// 
        /// Ejemplo: $2a$12$R9h/cIPz0gi.URNNX3kh2OPST9/PgBkqquzi.Ss7KIUgO2t0jWMUW
        /// </returns>
        /// <exception cref="ArgumentException">
        /// Se lanza cuando valorOriginal es null o vacío
        /// </exception>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Se lanza cuando el costo está fuera del rango válido (4-31)
        /// </exception>
        public static string Encriptar(string valorOriginal, int costo = CostoPorDefecto)
        {
            // VALIDACIONES DE ENTRADA
            if (string.IsNullOrWhiteSpace(valorOriginal))
                throw new ArgumentException("El valor a encriptar no puede estar vacío.", nameof(valorOriginal));

            // BCrypt requiere costo entre 4 (muy rápido, inseguro) y 31 (extremadamente lento)
            if (costo < 4 || costo > 31)
                throw new ArgumentOutOfRangeException(nameof(costo), costo, 
                    "El costo de BCrypt debe estar entre 4 y 31. Recomendado: 10-15.");

            try
            {
                // GENERACIÓN DEL HASH BCRYPT
                // BCrypt.HashPassword maneja automáticamente:
                // - Generación de salt aleatorio único
                // - Aplicación del algoritmo de hashing
                // - Formateo del resultado en estándar BCrypt
                return BCrypt.Net.BCrypt.HashPassword(valorOriginal, costo);
            }
            catch (Exception excepcion)
            {
                // Capturar cualquier error interno de BCrypt y re-lanzar con contexto útil
                throw new InvalidOperationException(
                    $"Error al generar hash BCrypt con costo {costo}: {excepcion.Message}",
                    excepcion
                );
            }
        }

        /// <summary>
        /// Verifica si un valor corresponde a un hash BCrypt específico.
        /// 
        /// Esta es la única forma de "verificar" datos hasheados con BCrypt, ya que
        /// el proceso es irreversible. No se puede obtener el valor original.
        /// 
        /// Proceso de verificación:
        /// 1. Extrae el salt y costo del hash existente
        /// 2. Aplica el mismo proceso de hashing al valor a verificar
        /// 3. Compara el resultado con el hash original
        /// 4. Devuelve true si coinciden exactamente
        /// </summary>
        /// <param name="valorOriginal">Valor a verificar (texto plano)</param>
        /// <param name="hashExistente">Hash BCrypt existente para comparar</param>
        /// <returns>
        /// true si el valor corresponde al hash
        /// false si no coinciden o hay algún error
        /// </returns>
        /// <exception cref="ArgumentException">
        /// Se lanza cuando algún parámetro es null o vacío
        /// </exception>
        public static bool Verificar(string valorOriginal, string hashExistente)
        {
            // VALIDACIONES DE ENTRADA
            if (string.IsNullOrWhiteSpace(valorOriginal))
                throw new ArgumentException("El valor a verificar no puede estar vacío.", nameof(valorOriginal));

            if (string.IsNullOrWhiteSpace(hashExistente))
                throw new ArgumentException("El hash existente no puede estar vacío.", nameof(hashExistente));

            try
            {
                // VERIFICACIÓN SEGURA CON BCRYPT
                // BCrypt.Verify maneja automáticamente:
                // - Extracción de salt y costo del hash existente
                // - Re-hashing del valor original con los mismos parámetros
                // - Comparación segura constante en tiempo (evita timing attacks)
                return BCrypt.Net.BCrypt.Verify(valorOriginal, hashExistente);
            }
            catch (Exception)
            {
                // Si ocurre cualquier error (hash mal formado, etc.), retornar false
                // No lanzar excepción para que la verificación falle gracefully
                return false;
            }
        }

        /// <summary>
        /// Verifica si un hash BCrypt necesita ser re-hasheado debido a cambio de costo.
        /// 
        /// Útil para migrations de seguridad cuando se quiere aumentar el costo
        /// computacional de hashes existentes en la base de datos.
        /// </summary>
        /// <param name="hashExistente">Hash BCrypt a verificar</param>
        /// <param name="costoDeseado">Nuevo costo deseado</param>
        /// <returns>
        /// true si el hash debe ser re-generado con el nuevo costo
        /// false si ya tiene el costo deseado
        /// </returns>
        public static bool NecesitaReHasheo(string hashExistente, int costoDeseado = CostoPorDefecto)
        {
            if (string.IsNullOrWhiteSpace(hashExistente))
                return true;

            try
            {
                // Extraer el costo actual del hash existente
                // Formato BCrypt: $2a$12$... donde 12 es el costo
                if (hashExistente.Length >= 7 && hashExistente.StartsWith("$2"))
                {
                    string costoParte = hashExistente.Substring(4, 2);
                    if (int.TryParse(costoParte, out int costoActual))
                    {
                        return costoActual < costoDeseado;
                    }
                }
                
                // Si no se puede parsear, asumir que necesita re-hasheo
                return true;
            }
            catch
            {
                // En caso de error, asumir que necesita re-hasheo
                return true;
            }
        }
    }
}

// NOTAS PEDAGÓGICAS para el tutorial:
//
// 1. ESTA ES UNA CLASE DE UTILIDAD (HELPER):
//    - Métodos estáticos que no requieren instancia
//    - Se enfoca únicamente en operaciones de BCrypt
//    - Se puede usar desde cualquier parte del sistema
//    - No mantiene estado, solo proporciona funcionalidad
//
// 2. ¿POR QUÉ UNA CLASE SEPARADA?
//    - SRP: Separar responsabilidades de encriptación del resto de lógica
//    - Reutilización: Se puede usar en servicios, repositorios, controladores
//    - Testing: Fácil de probar independientemente
//    - Mantenimiento: Cambios en encriptación se centralizan aquí
//
// 3. SEGURIDAD DE BCRYPT APLICADA:
//    - Costo 12: Balance entre seguridad (150ms) y usabilidad
//    - Salt automático: Cada hash es único incluso con la misma entrada
//    - Verificación constante en tiempo: Evita timing attacks
//    - Manejo seguro de errores: No exponer información sensible
//
// 4. CASOS DE USO EN LA API:
//    - Hashear contraseñas antes de guardar en BD
//    - Verificar login de usuarios
//    - Hashear PINs o códigos de seguridad
//    - Cualquier campo que requiera verificación sin recuperación
//
// 5. NO USAR BCRYPT PARA:
//    - Datos que necesites recuperar (emails, nombres, direcciones)
//    - Datos que requieran búsquedas parciales
//    - IDs o códigos que necesites mostrar al usuario
//    - Datos que se procesen frecuentemente en lotes
//
// 6. PRÓXIMO PASO EN EL TUTORIAL:
//    Usar esta clase en el repositorio para encriptar campos especificados
//    antes de la inserción en base de datos.