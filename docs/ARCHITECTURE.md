# ADR

Dieses Dokument hält die wichtigsten Design und Architektur Entscheidungen für das Projekt fest und wird stetig aktualisiert.

## 1. Paket- und Dependency-Management --> **uv**

Ich möchte einfach mal `uv` verwenden, da aktuell ein Hype darum existiert.

## 2. Datenbank
- **SQLite:** Die Datenmengen sind überschaubar. Tabellen müssen stetig erweitert werden, und für die Analyse/Modelling müssen die Tabellen in verschiedenen Aggregationen ausgelesen werden können, was sich gut mit SQL umsetzen lässt.
- **SQLAlchemy 2.0:** Setzt den Grundstein für eine spätere Umstellung auf eine andere/externe SQL-Datenbank (falls notwendig/sinnvoll). ...Außerdem ist es der pythonische Ansatz...

## 3. Datenbeschaffung (API Fetching)
- **`openmeteo-requests`** für historische Wetterdaten, sowie Wettervorhersagen UND Historische Wettervorhersagen.
- **`requests`** für Strompreise von `energy-charts.info`.
- Sonstiges: `requests-cache`, `retry-requests` für effiziente/schonende API-Anfragen.



