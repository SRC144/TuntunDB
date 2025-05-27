# Proyecto 1 - Base de datos II 

## Descripción
Sistema de gestión de bases de datos desarrollado para el curso de Base de Datos II, implementando una arquitectura de tres capas con énfasis en la implementación de indices en la capa del motor de base de datos.

## Autores
- Sergio Cortez
- Carlos Sobenes

## Arquitectura
```
.
├── src/
│   ├── db/           # Motor de base de datos
│   ├── api/          # API REST en Flask
│   └── frontend/     # Interfaz de usuario en React
```

## Diagrama de Flujo de Consultas
![Diagrama de flujo del sistema](bd2_proyect.png)

### Flujo de Consultas
1. **Interfaz de Usuario** → El usuario interactúa con la interfaz, que envía consultas SQL o archivos CSV mediante llamadas HTTP.

2. **Capa API** → El servidor Flask actúa como intermediario:
   - Recibe y valida las peticiones
   - Encapsula la comunicación con el motor de base de datos
   - Gestiona las respuestas y errores

3. **Motor de Base de Datos** → El procesamiento sigue una ruta bien definida:
   - El `QueryHandler` recibe la consulta y actúa como punto de entrada
   - El parser analiza la sintaxis y genera una representación estructurada
   - El query runner ejecuta el comando asociado a la consulta procesada.
   - El comando correspondiente:
     * Selecciona la estrategia de acceso (índices o secuencial) 
     * Realiza operaciones sobre los datos
     * Gestiona el almacenamiento en disco (Almacenamos en el directorio src/data)
   - Para importación CSV:
     * Se analiza la estructura del archivo
     * Se crea una tabla con el esquema apropiado
     * Importamos los datos optimizando el almacenamiento
     * Proporcionamos el script `create_from_files.py` para probar esta funcionalidad

4. **Flujo de Retorno** → Los resultados atraviesan el camino inverso:
   - El motor empaqueta los resultados
   - La API los formatea en JSON
   - La interfaz los presenta de manera amigable

## Componentes Principales

### Motor de Base de Datos
- Operaciones CRD (No se implemento comando update)
- Indexación:
  - B+Tree
- Tipos de datos soportados: INT, VARCHAR, DATE, ARRAY
- Importación desde CSV con inferencia de tipos
- Estrategia de reindexación:
  - Mantiene índices actualizados durante operaciones DELETE
  - Reconstrucción completa de datos e índices cuando los registros marcados como eliminados alcanzan el 20%

### API REST
- Endpoints para operaciones de base de datos
- Validación y gestión de errores

### Frontend
- Interfaz interactiva en React
- Proporcionamos consultas ejemplo que se pueden cargar de manera interactiva
- Editor de consultas SQL
- Visualización de resultados

## Tecnologías
- Backend: Python (Flask)
- Frontend: React + Vite
- Estilos: Tailwind CSS

## Requisitos
- Para el backend crear un entorno python e installar `requirements.txt` con `pip install -r requirements.txt`. Iniciar el servidor con `src/run.py` .
- Para el frontend ir a src/frontend e instalar dependencias con `npm install package.json`. Luego iniciar servidor con `npm start`.

## Ejemplo de Uso
```sql
-- Crear tabla desde CSV
create table cancer from file 'cancer_data.csv;

-- Consultas típicas
CREATE TABLE Usuarios (...);
SELECT * FROM Usuarios WHERE edad BETWEEN 25 and 33;
```
## [Enlace al Video](https://www.youtube.com/watch?v=4qRWlyFtNLA)
## Referencias de literatura para el B+ e idea del Extendible Hash (El último no integrado al gestor)
- Elmasri, R., & Navathe, S. B. (2015). Fundamentals of database systems (7th ed.). Pearson. (Insercion y busqueda del B+)
- Jannink, J. (1995). Implementing deletion in B+-trees. ACM SIGMOD Record, 24(1), 33–38. [](https://doi.org/10.1145/202660.202666)
- Fagin, R., Nievergelt, J., Pippenger, N., & Strong, H. R. (1979). Extendible hashing—a fast access method for dynamic files. ACM Transactions on Database Systems, 4(3), 315–344. [](https://doi.org/10.1145/320083.320092)
