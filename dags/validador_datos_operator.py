from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd

class ValidadorDatosOperator(BaseOperator):
    """
    Operador personalizado para validar calidad de datos en archivos CSV.
    """

    @apply_defaults
    def __init__(self, archivo_entrada, umbral_calidad=0.9, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.archivo_entrada = archivo_entrada
        self.umbral_calidad = umbral_calidad

    def execute(self, context):
        self.log.info(f"Validando archivo: {self.archivo_entrada}")

        try:
            df = pd.read_csv(self.archivo_entrada)
        except Exception as e:
            raise Exception(f"Error leyendo archivo: {e}")

        total = len(df)
        completos = df.dropna().shape[0]
        calidad = completos / total

        self.log.info(f"Calidad de datos: {calidad:.2%}")

        if calidad < self.umbral_calidad:
            raise Exception("Calidad de datos insuficiente")

        return {
            "total_registros": total,
            "registros_validos": completos,
            "calidad": calidad
        }

