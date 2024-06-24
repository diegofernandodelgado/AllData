Buen día,

Inicialmente quise adelantar el punto 1 usando totalmente Databricks, por ello cree una sesión  en https://community.cloud.databricks.com/,  tuve dificultades para la lectura del archivo csv desde GitHub, luego de crear token con permisos sobre el repo ejecutar sin errores, pude observar que la carpeta no se estaba creando en DBFS de databricks. 

Acto seguido ejecute el paso 1 desde anaconda Jupyter lab en ambiente local, de ello se entrega el notebook "punto 1 en Jupyter.ipynb", luego de crear los csv después de procesar los datos "00 home Jupyter local.jpg", que para mi ejemplo se puede simular al caso cuando XM dispone de algunos archivos sobre consumos y estos son cargados con los procedimientos respectivos a las aplicaciones, de esta misma manera podría diseñarse script que solucionen esta actividad y apoyar en el gobierno de datos.  Los archivos se exportaron en local de Jupyter anaconda, no logré en el tiempo del test subirlo en azure blob storage, allí quedo el error generado.

Posterior a esto con el desafío de cargarlos dataframe o archivos en una base de datos SQL/NOSQL,  a traves de databricks Catalog, se cargaron los archivos generados en el desafio 2 "00 Carga de Archivos.jpg", siendo estos accesibles al estar en el ambito de databricks, por lo cual nuevamente  replique la creacion de los data frame, la carga en tablas temporales y la creación de tablas delta para desde pyspark consultar con SQL, y asi construir la vista solicitada PUNTO 1 CON DATABRICKS.

Hasta el final de las pruebas he tratado de realizar la APIREST con el framework FASTAPI para python, hasta ahora sin éxito :(.

Para finalizar se presenta el archivo "Punto 2  Analisis de Ventas.pbix" de PowerBI donde se resuelve el punto 2 con un dashboard sencillo que hace BI sobre el resultado de las ventas por período y algunos aspectos relevantes del proceso.


Mi apreciación es que los servicios fundatión o de gratuidad para practicas eliminan servicios de gran utilidad, y alli es donde se hace necesario contar con la version completa del producto y su respectivo soporte, para asi lograr las tareas podersas de estas tecnologias.
