# Entity-Relationship Diagram (ERD)

The following diagram represents the normalized schema in DuckDB with [Mermaid](https://mermaid.js.org/syntax/entityRelationshipDiagram.html) code:

```mermaid
erDiagram
    USERS {
        int user_id PK
        string first_name
        string last_name
        string email
        string phone
        int location_id FK
        int company_id FK
    }
    LOCATIONS {
        int location_id PK
        float latitude
        float longitude
    }
    COMPANIES {
        int company_id PK
        string company_name
        string company_industry
    }
    WEATHER {
        int location_id FK
        float weather_temperature
        float weather_windspeed
    }

    USERS ||--|| LOCATIONS : "lives at"
    USERS ||--|| COMPANIES : "employed by"
    LOCATIONS ||--|| WEATHER : "weather info for"
```

I pasted this code into the [Mermaid Live Editor](https://mermaid.live/) and then exported the diagram as an image that I saved in the same directory as this file.