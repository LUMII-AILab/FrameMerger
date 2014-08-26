# LETA UI webservera konfigurācija

databases = [
    ["default", {
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

# service_port = 9990         # konsolidātora un db upload servisa ports

# host = 'localhost'          # hostname, kas sasniedzams no ārpuses (no pārlūka)
# port = 9000                 # webservera ports, default: 9000
# api_port = 9900             # ports, kas tiks izmantots API datu apmaiņai, default: 9900
# listen_host = '0.0.0.0'     # kurus hostus klausās webserveris, default: 0.0.0.0 => visi interfeisi

# Pēc servera palaišanas pārlūkā: http://[host]:[port] 
