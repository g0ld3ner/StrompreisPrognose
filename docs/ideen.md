- Historische Wetterdaten vs. Historische Wettervorhersagen
    - alle 6 Stunden die (historische) Wettervorhersage für die nächsten 7 Tage abfragen
        - rückwirkend für 365 Tage (Trainingshorizont für das ML-Modell)
            - sonst nur erweitern
        - T-ref immer 00:00 UTC, dann +6h, +12h, +18h
    --> große Datenmengen (2D-Matrix)
        --> direkt nur die aggregierten Werte nach gewichteten locations abspeichern?, oder egal?
            - dann können nur aggregierte Werte für Forcast und History verglichen werden

==> Das Modell braucht für das Trining nur Historische Wetterdaten, keine Vorhersagen!
    --> also Historische Wettervorhersagen nur alle 6 Stunden für 7 Tage für die letzten X Tage abfragen.
        - ab da an dann kontinuierlich arbeiten?
