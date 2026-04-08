Buen día,

Sobre las herramientas:  en las acciones de la automatización lo primero será definir las conexiones  a bases de datos, donde vamos a necesitar datos de credenciales.

En un segundo momento se procede cono la extracción de la data, teniendo como parámetro de selección de data, la tabla proporcionada y la ultima fecha es decir basado last sysdate desde la fuente oracle.

seguido se hara la validación del Schema, para verificar correspondencia de los datos, posición, estructura, metadata.

Para finalizar, se hace el volcado de la data en postgres, y en el proceso de escritura se define la cantidad de registros por operación para evitar colapsos o interrupciones.

como utilitarias se tienen
test de lectura
test de extracción
loggers 
Retry


agregando el main.py al automatizar de tareas disponible y los elementos adicionales se podrá ejecutar la tarea automatizada según su posición u orden de calendario.

El ingeniero de dato, o analista de datos podrá validar su correcto traslado de datos al finalizar las tareas del main.py
