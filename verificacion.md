Ejercicio: Crear DAG con operadores y sensores

Verificación: 
1. ¿En qué situaciones usarías un sensor en lugar de ejecutar tareas inmediatamente? 
2. ¿Cuáles son las ventajas de crear operadores personalizados?

Respuestas:

1. ¿En qué situaciones usarías un sensor en lugar de ejecutar tareas inmediatamente?
Usaría un sensor cuando el flujo de trabajo depende de que ocurra algo externo antes de continuar, por ejemplo:
- La llegada de un archivo (como el FileSensor usado en el ejercicio).
- La disponibilidad de datos en una base de datos.
- La finalización de otro proceso o DAG.

En este ejercicio, el sensor es necesario porque el pipeline no debe procesar ni validar datos hasta que exista el archivo datos_ventas.csv. Sin el sensor, las tareas fallarían al intentar usar un archivo que aún no existe.

2. ¿Cuáles son las ventajas de crear operadores personalizados?
Crear operadores personalizados permite:
- Reutilizar lógica de negocio (por ejemplo, validación de calidad de datos) en distintos DAGs.
- Mantener el DAG más limpio y legible, separando la lógica compleja del flujo.
- Estandarizar procesos (validaciones, transformaciones, controles) dentro del equipo.
- Facilitar mantenimiento y escalabilidad del proyecto.

En este ejercicio, el ValidadorDatosOperator encapsula la validación de calidad del archivo CSV, evitando repetir ese código en cada DAG y asegurando que solo se procesen datos confiables.

Conclusión:
- Los sensores controlan cuándo se ejecutan las tareas según condiciones externas.
- Los operadores personalizados definen cómo se ejecuta una lógica específica de forma reutilizable y profesional.