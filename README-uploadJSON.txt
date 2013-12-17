Marķēto dokumentu ielādēšana semantiskajā datubāzē
--------------------------------------------------

Priekšnosacījumi:
	Python 2.7.6
	Python modulis psycopg2 (uz ubuntu instalējas ar "apt-get install python-psycopg2" / "pip install psycopg2")

Konfigurēšana:
	Jānorāda datubāzes servera pieslēguma informācija failā 'db_config.py' - skat 'db_config.py.template'

Darbināšana:
	./uploadJSON.py
	Pa STDIN nodod apstrādājamo failu nosaukumus, pa vienam nosaukumam rindiņā. Failiem jābūt 'melnās kastes' veidotajā JSON formātā.