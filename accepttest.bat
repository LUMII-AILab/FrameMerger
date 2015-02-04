
py ./entity_maintenance.py --cleardb --database=accept_test

type nul > ./testdata/jsonlist.txt
:: Vienkāršais risinājums absolūtajām adresēm, strādā testdata apakšmapēm.
::dir /A:-d /B /S testdata\*.json >> ./testdata/jsonlist.txt
::dir /A:-d /B /S testdata\*.json.gz >> ./testdata/jsonlist.txt
:: Čakarīgais risinājums, lai būtu relatīvās adreses, nestrādā apakšmapēm.
:: Paldies, http://stackoverflow.com/a/12209020
@echo off
setlocal disableDelayedExpansion
for /f "delims=" %%A in ('forfiles /s /m *.json /p testdata /c "cmd /c echo @relpath"') do (
  set "file=%%~A"
  setlocal enableDelayedExpansion
  echo ./testdata/!file:~2! >> ./testdata/jsonlist.txt
  endlocal
)
for /f "delims=" %%A in ('forfiles /s /m *.json.gz /p testdata /c "cmd /c echo @relpath"') do (
  set "file=%%~A"
  setlocal enableDelayedExpansion
  echo ./testdata/!file:~2! >> ./testdata/jsonlist.txt
  endlocal
)
endlocal
@echo on

py ./uploadJSON.py --database=accept_test < ./testdata/jsonlist.txt

py ./consolidate_frames.py --database=accept_test --dirty