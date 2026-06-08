import json
from collections import Counter

with open('standalone_10m.json', 'r') as f:
    data = json.load(f)

labels = data['labels']
counts = Counter(labels)

print(f"Total nodes: {len(labels)}")
print("Label Distribution:")
for label, count in sorted(counts.items()):
    label_name = "UNKNOWN" if label == 4294967295 else label
    print(f"  Label {label_name}: {count}")
