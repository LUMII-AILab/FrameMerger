Moduļi darbam ar semantisko datubāzi
------------------------------------

Priekšnosacījumi:
	Python 2.7.6
	Python modulis psycopg2 (uz ubuntu instalējas ar "apt-get install python-psycopg2" / "pip install psycopg2")
	Python modulis openpyxl ("sudo pip install openpyxl", ar jaunākām versijām nedarbojas)

Konfigurēšana:
	Jānorāda datubāzes servera pieslēguma informācija failā 'db_config.py' - skat 'db_config.py.template'

Saskarne ar moduļiem paredzēta caur unix stdin/stdout - pēc moduļa palaišanas nododot apstrādājamo datu ID. Pēc katra fragmenta veiksmīgas apstrādes moduļi izvada paziņojumu ar attiecīgo ID.


Marķēto dokumentu ielādēšana semantiskajā datubāzē
--------------------------------------------------

Darbināšana notiek caur
	./uploadJSON.py
Pa STDIN nodod apstrādājamo failu nosaukumus, pa vienam nosaukumam rindiņā. Failiem jābūt 'melnās kastes' veidotajā JSON formātā - vai nu tīrā veidā ar paplašinājumu .json, vai arī katram failam atsevišķi gzip'otam ar paplašinājumu json.gz
Darbināšanas piemērs testam:
	./uploadJSON.py < tests/test_json_list
Paredzētais izvads uz stdout:
	/Users/pet/Documents/LETA/staging/tests/p2/0A8AE6CC-ACA4-4677-B217-A6FD383017F4.json	OK
	/Users/pet/Documents/LETA/staging/tests/p2/0C4FABDC-A6F9-48DD-AB9E-1FAFA620EA08.json	OK
	/Users/pet/Documents/LETA/staging/tests/p2/2CC3CE6B-3548-413E-95CD-3F011DD856DD.json	OK


Faktu summarizācija
-------------------

Faktu summarizācija notiek porcijās pa faktiem, kas piemin kādu konkrētu entītijas ID.
Darbināšana notiek caur
	./consolidate_frames.py 
Pa STDIN nodod apstrādājamo entītiju ID, pa vienam ID katrā rindiņā. 
Darbināšanas piemērs testam:
	./consolidate_frames.py < tests/test_entity_list 
Paredzētais apstrādes rezultāts:
	1575673	OK
	1575689	OK
	1575990	OK
	1576028	OK
	1576160	OK

Verbalizācijas webserviss
-------------------------

Palaišana:
	./verbservice.py
Piekļuve:
	[servera adrese]:9000/verbalize/[summary freima ID]
	http://localhost:9000/verbalize/1376556
	Uz GET pieprasījumiem šādā formā tiks atgriezts verbalizācijas teksts norādītajam summary freima ID, ņemot aktuālos datus no datubāzes, kas ir db_config.py


Konsolidācijas webserviss (tiek izsaukts no UI)
-----------------------------------------------

Konfigurēšana:
	config_template.py => config.py + parametru iestādīšana (host+port+datubāzēm ir jāsakrīt ar atbilstošo UI konfigurāciju)
	db_config.py.template => db_config.py + parametru iestatīšana (datubāze jeb api_conn_info šeit var nebūt konfigurēta)
Palaišana:
	./service.py
	vai
	./run_service.sh    šajā gadījumā ir iespēja pārstartēt servisu ar kill -TERM `cat service.pid`
