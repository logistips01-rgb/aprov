"""Catálogos cerrados del módulo de Incidencias de Transporte y Devoluciones.

Único punto de verdad para los enums usados en registro, informe y export.
No importar nada de app48.py aquí: este módulo es completamente aislado.
"""

MOTIVOS = [
    "Temperatura / cadena de frío",
    "Retraso en entrega",
    "Mercancía dañada o palet volcado",
    "Error de referencia o picking",
    "Faltas",
    "Excesos",
    "Etiquetado",
    "Remontaje incorrecto",
    "Documentación",
    "Rechazo en muelle",
    "Otros",
]

RESPONSABILIDADES = ["Transporte", "Producción", "Almacén", "Cliente", "Indeterminada"]

ESTADOS = ["Abierto", "En gestión", "Resuelto", "Abonado"]

# Nombre de la colección Firestore (un documento por incidencia) y del
# documento especial que acumula el catálogo abierto de transportistas/clientes.
COLECCION = "incidencias_transporte"
DOC_CATALOGO = "_catalogo"

COLOR_ESTADO = {
    "Abierto":     "#C0392B",
    "En gestión":  "#D68910",
    "Resuelto":    "#2980B9",
    "Abonado":     "#1E8449",
}

COLOR_CORPORATIVO = "#C8102E"
COLOR_OSCURO = "#1E2A3A"
