# Konsolidācijas webservisa konfigurācija

databases = [
    ["database1", {
        "host":     "<hostname>", 
        "port":     5555, 
        "dbname":   "<dbname>", 
        "user":     "<user>", 
        "password": "<password>"
    }],
    ["database2", {
        "host":     "<hostname>", 
        "port":     5555, 
        "dbname":   "<dbname>", 
        "user":     "<user>", 
        "password": "<password>"
    }]
]

log_dir = 'log'

# service_port = 9990         # konsolidātora un db upload servisa ports
# listen_host = '0.0.0.0'     # kurus hostus klausās webserveris, default: 0.0.0.0 => visi interfeisi
