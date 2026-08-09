# Sistema de Diseño — Aldelis Aprovisionamiento
> Documento generado para revisión de diseño. Aplicación web construida con Streamlit (Python).

---

## 1. Paleta de Colores

### Colores Primarios

| Nombre | HEX | Uso |
|--------|-----|-----|
| Rojo Principal | `#E74C3C` | Botones primarios, acciones, acento |
| Rojo Hover | `#C0392B` | Estado hover del rojo principal |
| Amarillo | `#F39C12` | Estados en transición, avisos |
| Verde | `#27AE60` | Estados OK, activos, confirmaciones |

### Colores Neutros

| Nombre | HEX | Uso |
|--------|-----|-----|
| Fondo de App | `#ECF0F1` | Fondo general de la aplicación |
| Fondo Sidebar | `#2C3E50` | Sidebar de navegación |
| Texto Principal | `#2C3E50` | Títulos y textos principales |
| Texto Secundario | `#7F8C8D` | Labels, textos de apoyo |
| Texto Sidebar | `#BDC3C7` | Texto e iconos sobre sidebar oscuro |
| Header Tabla | `#5D6D7E` | Cabeceras de tabla, uppercase |
| Fondo Header Tabla | `#F4F6F7` | Fila de cabecera en tablas |
| Borde | `#D5D8DC` | Bordes y divisores |
| Fondo Secundario | `#FAFAFA` | Fondos alternativos |
| Blanco | `#FFFFFF` | Tarjetas, inputs, fondos limpios |

### Colores de Estado (Badges)

| Estado | Fondo | Texto | Uso |
|--------|-------|-------|-----|
| Pendiente | `#FDEDEC` | `#C0392B` | Estado crítico / sin iniciar |
| En preparación | `#FEF9E7` | `#D68910` | Estado en proceso |
| Activo | `#EAFAF1` | `#1E8449` | Estado completado / OK |

### Colores por Rol de Usuario

| Rol | Color |
|-----|-------|
| Admin | `#E74C3C` |
| I+D | `#2980B9` |
| Almacén | `#27AE60` |

---

## 2. Tipografía

- **Fuente:** `Arial, sans-serif` (fuente del sistema, sin custom fonts)

| Elemento | Tamaño | Peso | Transformación | Letter-spacing |
|----------|--------|------|----------------|----------------|
| Título principal | 28px | 700 | normal | — |
| Título secundario | 22–24px | 600–700 | normal | — |
| Título dashboard | 18px | 700 | normal | — |
| Valor de métrica | 22px | 700 | normal | — |
| Texto cuerpo | 13px | 400 | normal | — |
| Datos de tabla | 12px | 400–600 | normal | — |
| Label de métrica | 10px | 600 | UPPERCASE | 0.04em |
| Label de formulario | 10px | 600 | UPPERCASE | 0.05em |
| Cabecera de tabla | 10px | 700 | UPPERCASE | 0.05em |
| Label sidebar | 9px | 700 | UPPERCASE | 0.1em |
| Badge / Pill | 10–11px | 500–600 | normal | — |

---

## 3. Espaciado y Bordes

### Espaciado base (padding / gap)

| Token | Valor |
|-------|-------|
| xs | 2px |
| sm | 4px |
| md | 6–8px |
| lg | 10–12px |
| xl | 14–16px |
| 2xl | 20–24px |
| 3xl | 32px |

### Border Radius

| Elemento | Radio |
|----------|-------|
| Botones, inputs, selectbox | 6px |
| Métricas, expanders, tarjetas | 8px |
| Tablas, contenedores grandes | 10px |
| Badges / Pills | 20px |
| Dots de estado | 50% (circular) |

### Sombras / Focus

| Estado | Sombra |
|--------|--------|
| Focus input | `0 0 0 2px rgba(231,76,60,0.15)` |
| Focus button | `0 0 0 3px rgba(231,76,60,0.3)` |

---

## 4. Componentes

### Sidebar
- Fondo oscuro `#2C3E50`, texto `#BDC3C7`
- Ancho fijo, sin borde derecho
- Logo en header: cuadro 32×32px, `border-radius: 8px`, fondo `#E74C3C`, letra "A" en blanco
- Nombre de app: 14px/600, subtítulo: 10px/#888
- Ítem de nav activo: franja izquierda 3px `#E74C3C`, fondo `rgba(231,76,60,0.15)`
- Badge de rol: fondo `rgba(255,255,255,0.06)`, border-radius 6px

### Botones

| Tipo | Fondo | Texto | Borde | Hover |
|------|-------|-------|-------|-------|
| Secundario (default) | `#FFFFFF` | `#555` | `#D5D8DC` | Fondo `#E74C3C`, texto blanco |
| Primario (submit) | `#E74C3C` | Blanco | ninguno | Fondo `#C0392B` |

- `border-radius: 6px`, `font-size: 13px`, `padding: 10px`

### Inputs y Formularios
- Fondo blanco, borde `0.5px solid #D5D8DC`, `border-radius: 6px`
- Focus: borde `#E74C3C` + sombra roja al 15%
- Labels: 10px, UPPERCASE, `#7F8C8D`

### Métricas / KPIs
- Tarjeta blanca, borde `#D5D8DC`, `border-radius: 8px`, padding `10px 14px`
- Valor: 22px / 700 / `#2C3E50`
- Label: 10px / 600 / UPPERCASE / `#7F8C8D`

### Tablas
- Contenedor: fondo blanco, `border-radius: 10px`, borde `#D5D8DC`
- Cabecera sticky: fondo `#F4F6F7`, texto `#5D6D7E` UPPERCASE 10px
- Borde inferior cabecera: 2px `#D5D8DC`
- Filas alternas: fondo `#FEF9F9` (rojo) o `#F0FBF4` (verde) según contexto
- Separador de filas: `1px solid #F2F3F4`
- Max height: 520px con scroll vertical

### Badges de Estado
- `border-radius: 20px`, padding `3px 8px`
- Dot circular opcional a la izquierda
- Tres variantes: rojo / amarillo / verde (ver sección 1)

### Expanders / Acordeones
- Fondo blanco, borde `#D5D8DC`, `border-radius: 8px`

---

## 5. Estructura General de la App

```
┌──────────────────────────────────────────────────┐
│  Header (fondo #ECF0F1, borde inferior #D5D8DC)  │
├──────────┬───────────────────────────────────────┤
│          │                                       │
│ Sidebar  │  Área principal                       │
│ #2C3E50  │  Fondo #ECF0F1                        │
│          │                                       │
│  Nav     │  Tarjetas / Tablas / Formularios      │
│  items   │  en blanco sobre fondo gris claro     │
│          │                                       │
└──────────┴───────────────────────────────────────┘
```

---

## 6. Colores en Exportación Excel

| Zona | Fondo | Texto |
|------|-------|-------|
| Cabecera | `#2C3E50` | Blanco |
| Fila normal | `#FFFFFF` | — |
| Fila alterna | `#F7F8FA` | — |
| Celda roja | `#FCE4E6` | `#7B241C` |
| Celda amarilla | `#FEF4CC` | `#7D6608` |
| Celda verde | `#D6F0E0` | `#1A5E38` |

---

## 7. Notas para el Diseñador

- La app está construida en **Streamlit** (framework Python), por lo que el margen de personalización visual tiene límites técnicos (no es React/Vue). Sin embargo, se inyecta CSS personalizado.
- El acento de color corporativo es **rojo `#E74C3C`** sobre fondo neutro gris claro.
- El sidebar usa un tono oscuro navy `#2C3E50` como contraste.
- El sistema de estados tiene 3 niveles: rojo (pendiente), amarillo (en proceso), verde (OK).
- No hay custom fonts; se usa Arial del sistema.
- No hay iconos vectoriales (SVG) ni librería de iconos; se usan caracteres unicode (`&#9679;`, `&#128722;`, etc.).
- La tipografía es muy compacta (10px en labels), orientada a densidad de información.
