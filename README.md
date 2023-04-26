```json
{
    "env": {
        "SERVICE_RMQ_QUEUE": "iLogic.Messages",
        "ASD_POSTGRES_DBNAME": "poly_arch",
        "SERVICE_PG_PROCEDURE": "call device.check_section($1, $2)",
        "SERVICE_PG_GETOFFSET"="SELECT device.get_offset()",
    }
}
```