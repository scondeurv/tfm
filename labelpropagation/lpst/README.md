# Label Propagation Algorithm in Rust

Una implementación del algoritmo de propagación de etiquetas (Label Propagation) en Rust para clasificación semi-supervisada en grafos.

## Descripción

El algoritmo de propagación de etiquetas es un método semi-supervisado que propaga etiquetas a través de un grafo basándose en la estructura de conexiones. Los nodos con etiquetas conocidas propagan su información a nodos vecinos sin etiquetar.

### Características

- Implementación eficiente en Rust
- Soporte para grafos no dirigidos
- Convergencia configurable
- Exportación de resultados a JSON
- Generador de grafos con múltiples topologías
- Tests unitarios incluidos

## Instalación

```bash
cd /home/sergio/src/tfm/labelpropagation/lpst
cargo build --release
```

## Uso

### 1. Generar un grafo

El generador de grafos soporta varios tipos de topologías:

```bash
# Generar un grafo con estructura de comunidades
cargo run --release --bin generate_graph community 100 3 0.1 graph.json

# Generar un grafo aleatorio
cargo run --release --bin generate_graph random 100 3 0.1 graph.json

# Generar un grafo en malla
cargo run --release --bin generate_graph grid 100 3 0.1 graph.json

# Generar un grafo en anillo
cargo run --release --bin generate_graph ring 100 3 0.1 graph.json

# Generar un grafo small-world
cargo run --release --bin generate_graph smallworld 100 3 0.1 graph.json
```

**Parámetros:**
- `<type>`: Tipo de grafo (random, grid, ring, smallworld, community)
- `<num_nodes>`: Número de nodos
- `<num_labels>`: Número de etiquetas diferentes
- `<label_percentage>`: Porcentaje de nodos etiquetados inicialmente (0.0-1.0)
- `<output_file>`: Archivo JSON de salida

### 2. Ejecutar Label Propagation

```bash
cargo run --release --bin run_label_propagation graph.json 100 0.01 results.json
```

**Parámetros:**
- `<input_graph>`: Archivo JSON con el grafo
- `[max_iterations]`: Iteraciones máximas (default: 100)
- `[convergence_threshold]`: Umbral de convergencia (default: 0.01)
- `[output_file]`: Archivo de resultados (default: results.json)

### 3. Ejemplo simple (demo integrado)

```bash
cargo run --release
```

## Tipos de Grafos

### Random
Grafo aleatorio con probabilidad de conexión de 0.3 entre cada par de nodos.

### Grid
Estructura de malla 2D donde cada nodo se conecta con sus vecinos arriba, abajo, izquierda y derecha.

### Ring
Grafo en forma de anillo donde cada nodo se conecta con el siguiente formando un círculo.

### Small-World
Grafo de Watts-Strogatz que combina alta conectividad local con algunas conexiones de largo alcance.

### Community
Grafo con estructura de comunidades donde los nodos dentro de la misma comunidad están altamente conectados, con conexiones esporádicas entre comunidades.

## Formato de Archivos

### Entrada (graph.json)
```json
{
  "edges": [[0, 1], [1, 2], [2, 3]],
  "labeled_nodes": {
    "0": 0,
    "3": 1
  },
  "num_nodes": 100
}
```

### Salida (results.json)
```json
{
  "labels": {
    "0": 0,
    "1": 0,
    "2": 1,
    "3": 1
  },
  "iterations": 5,
  "converged": true
}
```

## Ejemplos de Uso Completo

### Ejemplo 1: Grafo pequeño con comunidades
```bash
# Generar grafo de 50 nodos con 2 comunidades
cargo run --release --bin generate_graph community 50 2 0.1 small_graph.json

# Ejecutar propagación
cargo run --release --bin run_label_propagation small_graph.json
```

### Ejemplo 2: Grafo grande con múltiples etiquetas
```bash
# Generar grafo de 1000 nodos con 5 etiquetas
cargo run --release --bin generate_graph smallworld 1000 5 0.05 large_graph.json

# Ejecutar propagación con más iteraciones
cargo run --release --bin run_label_propagation large_graph.json 200 0.005 large_results.json
```

## Tests

```bash
cargo test
```

## Complejidad

- **Tiempo**: O(I × E), donde I es el número de iteraciones y E el número de aristas
- **Espacio**: O(V + E), donde V es el número de vértices y E el número de aristas

## Referencias

- Zhu, X., & Ghahramani, Z. (2002). Learning from labeled and unlabeled data with label propagation.
- Raghavan, U. N., Albert, R., & Kumara, S. (2007). Near linear time algorithm to detect community structures in large-scale networks.
- Watts, D. J., & Strogatz, S. H. (1998). Collective dynamics of 'small-world' networks.
