# Ejecución del Sistema ETL con Airflow & Docker

1. Primero crear una carpeta **.env**
2. Colocar dentro de la carpeta **.env**:
   - ACCESS_KEY
   - SECRET_ACCESS_KEY
3. Ejecutar:
   - `docker compose up -d`
   - *Si los servicios de Airflow no aparecen la primera vez, ejecutar nuevamente:*
     - `docker compose up -d`
4. Verificar que los servicios se levantaron:
   - `docker compose ps`
5. Cuando aparezcan los servicios de Airflow, inicializar la base de datos:
   - `docker compose exec airflow-webserver airflow db init`
6. Acceder a la consola del contenedor para generar la key:
   - `docker compose exec airflow-webserver bash`
7. En la consola de Airflow ejecutar:
   - `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`
8. El comando genera un código similar:
   - `u5zsDt8jDlTX1yigZ2jbHST58_elO9JdEvL8yebYkQ8=`
9. Reemplazar ese valor en **AIRFLOW_CORE_FERNET_KEY** dentro del archivo **docker-compose.yml**
10. Salir de la consola de Airflow:
    - `exit`
11. Reiniciar los servicios:
    - `docker compose down -v`
    - `docker compose up -d airflow-init`
    - `docker compose up -d`

---
> <span style="color: orange; font-weight: bold;">⚠️ NOTA:</span>  
> <span style="color: orange;">Crear todos los servicios en la misma region `us-east-1` tiene mejor compatibilidad y menor costo</span>
---
# PASO A PASO – GENERAR KEYS (ACCESS KEY / SECRET KEY)
1. **Crear un usuario en IAM**  
   - En la consola de AWS ve a **IAM → Users** y crea un nuevo usuario.
2. **Asignar permisos al usuario**  
   - Adjunta la política adecuada según lo que necesite el usuario.  
   - Ejemplo usado aquí `AmazonS3FullAccess`.  
   - *Nota:* Otorgar permisos mínimos necesarios por seguridad (principio de menor privilegio).
3. **Ir a Security credentials (credenciales de seguridad)**  
   - Dentro del detalle del usuario, abre la pestaña **Security credentials**.
4. **Generar una Access Key**  
   - Haz clic en **Create access key**.  
   - Selecciona el tipo de uso (por ejemplo **AWS CLI**) y asigna un nombre descriptivo para la key.
5. **Guardar la Access Key y Secret Access Key**  
   - AWS mostrará la **Access Key ID** y la **Secret Access Key** la secret sólo se muestra una vez asi que copienla.  
   - **Guárdalas en un lugar seguro** o descarga el archivo `.csv` que ofrece AWS.
---
# PASO A PASO – CREACIÓN DEL BUCKET S3
1. **Ingresar al servicio Amazon S3**  
    - Desde la consola de AWS, en el buscador escribe `S3`
    - Haz clic en **Amazon S3**.
2. **Crear un nuevo bucket**  
    - En la parte superior, haz clic en el botón **Create bucket**
3. **Configurar la sección General configuration**   
    - Seleccionar **General purpose**, ideal para data lakes
    - En **Bucket name**, escribe el nombre del bucket este tiene que ser unico globalmente
4. **Configurar Object Ownership**    
    - Selecciona **ACLs disabled**, si quieres que sean administrados por tu cuenta AWS
5. **Configurar Block Public Access**  
    - **Block all public access**, garantiza que tu bucket sea privado y seguro.
6. **Bucket Versioning**  
    - Selecciona **Disable**
7. **Tags**  
    - Puedes dejarlo vacío o agregar tags si deseas organizar costos, para esta pracitca lo dejaromos asi
8. **Default encryption**  
    - Dejamos esto por defecto
9. **Revisar la configuración**  
    - Revisa que todas las opciones sean correctas
10. **Crear el bucket**
    - Haz clic en el botón **Create bucket**
---
# PASO A PASO – CREACIÓN DE UNA DATABASE EN AWS GLUE DATA CATALOG
1. **Ingresar al servicio AWS Glue**  
    - Desde la consola de AWS, en el buscador escribe `Glue`
    - Haz clic en **AWS Glue**
2. **Crear una nueva database**  
    - En el menu lateral, haz clic en **Data CataloCreate -> Databases**
    - En la parte superior, haz clic en el botón **Add database**
3. **Configurar la database**
    - En **Database name** escribe el nombre de la base de datos que representará el catálogo de tus tablas
    - **Regla:** el nombre debe estar en minúsculas y sin espacios
    - La **Description** es opcional puedes agregar una descripción clara sobre el propósito de esta database.  
    - **Location** esta opción solo se usa si deseas que *todas* las tablas generadas tengan una ruta base fija en S3
4. **Crear la base**  
    - Revisa que los valores sean correctos y**Create database**
---
# PASO A PASO – CREACIÓN DE UNA DATABASE EN AWS CRAWLER**
1. **Ingresar al servicio AWS Glue**  
    - Desde la consola de AWS, en el buscador escribe `Glue`  
    - Haz clic en **AWS Glue**
2. **Crear un nuevo Crawler**  
    - En el menu lateral, haz clic en **Data CataloCreate -> Crawlers**
    - En la parte superior, haz clic en el botón **Create crawler**
3. **STEP 1 – Configurar propiedades del crawler**
    - En **Name** colocamos el nombre con el que identificaremos nuestro crawler.
    - En **Description** damos una breve descripción de lo que hace el crawler.
    - **Tags:** es opcional, pero aquí se pueden configurar etiquetas para clasificar mejor los recursos.
4. **STEP 2 – Choose data sources and classifiers**
    - **Is your data already mapped to Glue tables?** seleccionamos **Not yet**, esto indica que aún no existe un mapeo previo.
    - En **Añadir origen de datos**, presionamos el botón **Add a data source**, que abrirá una ventana:
        - Seleccionamos el origen de datos, que en este caso es un **S3**
        - En **Network connection**, que es opcional, lo dejamos vacío; esto sirve para conectar Glue a redes privadas mediante VPC
        - En **Location of S3 data**, tenemos dos opciones:
            - **In this account:** indica que el bucket S3 está en nuestra cuenta
            - **In a different account:** indica que el bucket S3 está en otra cuenta
          Seleccionamos la opción de nuestra cuenta
        - En **S3 path**, seleccionamos con **Browse S3** la ruta del bucket donde están nuestros datos.
        - Todo lo demás lo dejamos con los valores por defecto
    - **Custom classifiers – optional**
        - Lo dejamos con sus valores por defecto
5. **STEP 3 – Configure security settings**
    - En **IAM Role**, creamos un rol dedicado con **Create new IAM role**. Se abrirá una ventana donde asignamos un nombre o seleccionamos un rol existente
    - En **Lake Formation configurations**, lo dejamos por defecto; esta opción sirve para controlar permisos avanzados a nivel de tabla y columna
    - **Opciones avanzadas:** podemos configurarlas según necesidades específicas, pero para este caso las dejamos por defecto
6. **STEP 4 – Set output and scheduling**
    - En **Target database**, también se puede crear una database de Glue como la que creamos anteriormente. Esto ahorra tiempo, pero como ya existe, simplemente seleccionamos la base creada
    - En **Table name prefix**, podemos colocar un prefijo que se antepondrá al nombre de las tablas generadas
    - En **Maximum table threshold**, es opcional y lo dejamos vacío; aquí podríamos limitar cuántas tablas puede crear el crawler
    - Las opciones avanzadas las dejamos con sus valores por defecto.
    - En **Crawler Schedule**, configuramos la frecuencia de ejecución. Puede ser por hora, día, semana, mes o personalizado. Para nuestro caso, lo dejamos **On demand**
7. **Review and create**
    - Aquí solo revisamos que todo esté configurado correctamente antes de crear el crawler
8. **Una vez esté creado**
    - Solo ejecutamos el crawler para que procese los datos
---
# PASO A PASO – CONFIGURACIÓN DE AWS ATHENA
1. **Ingresar al servicio Amazon Athena**
    - Desde la consola de AWS, en el buscador escribe **Athena**
    - Haz clic en **Amazon Athena**
    - Verás la pantalla de bienvenida con el botón **Launch query editor**
2. **Abrir el Query Editor de Athena**
    - En la página principal de Athena, haz clic en **Launch query editor**
    - Se abrirá el editor donde escribirás tus consultas SQL
    - Si es la primera vez, Athena mostrará un mensaje indicando que debes configurar la **ubicación de resultados en S3**
3. **Configurar el bucket para resultados de consultas**
    - En la parte superior verás un mensaje azul indicando que debes configurar el **Query result location** 
    - Haz clic en **Edit settings** o **Manage**
4. **Configuración de Gestión**
    - En el campo **Location of query result**, ingresa o selecciona un bucket S3 donde Athena guardará los resultados. (Puedes crear uno núevo o seleccinar uno existente) 
    - En **Expected bucket owner (optional)** dejao vacio pero aqui se puede coloar el S3 de una cuenta exerna
    - Todo lo demas dejalo vacio o por defecto.
---
# PASO A PASO – CREACIÓN DE AWS QUICKSIGHT
1. **Ingresar al servicio Amazon QuickSight**
    - Desde la consola de AWS, en el buscador escribe **QuickSight** 
    - Haz clic en **QuickSight** para abrir el servicio
2. **Crear la cuenta de QuickSight**
    - Si es la primera vez, verás la opción **“Regístrese en Amazon Quick Suite”**
    - Completa los datos:
        - **Nombre de la cuenta:** un nombre único
        - **Correo de notificaciones:** tu email  
        - **Región:** selecciona la misma región donde levantamos los anteriores servicio
        - **Método de autenticación:** deja Inicio de sesión único o basado en contraseña
        - **Cifrado:** deja **Use AWS-managed key (Default)**
    - Haz clic en **Crear cuenta**
3. **Otorgar permisos para que QuickSight acceda a AWS**
    - En el panel izquierdo, ve a: **Administrar cuenta → Recursos de AWS**
    - Habilita acceso a los servicios necesarios:
        - **Amazon S3**
        - **Amazon Athena**
        - En **Amazon S3**, selecciona los buckets que necesites
    - Guarda los cambios.
4. **Crear un nuevo origen de datos**
    - Ve a la sección **Conjuntos de datos** en el menú izquierdo
    - Haz clic en **Crear conjunto de datos → Crear origen de datos**
    - Selecciona **Amazon Athena**
5. **Configurar la conexión con Athena**
    - Asigna un nombre para el origen
    - En **Grupo de trabajo de Athena**, selecciona **[ primary ]**
    - Haz clic en **Validar conexión**
    - Luego selecciona **Crear origen de datos**
6. **Seleccionar la tabla a visualizar**
    - En **Catálogo**, elige **AwsDataCatalog**
    - En **Base de datos**, la base que creaste 
    - En **Tablas**, selecciona detectada por el crawler  
    - Presiona **Seleccionar**
7. **Elegir el método de carga del conjunto de datos**
    - Dispondrás de dos opciones:
        - **Importar a SPICE** → más rápido y recomendado  
        - **Consulta directa sobre Athena**
    - Selecciona **Importar a SPICE** para un análisis más ágil.  
    - Haz clic en **Visualizar**.
8. **Crear la hoja de análisis**
    - QuickSight mostrará la ventana **Nueva hoja**.  
    - Selecciona:
        - **Hoja interactiva**  
        - **Diseño:** En mosaico  
        - **Resolución:** 1600px
    - Haz clic en **Crear**.
9. **Construir el dashboard**
