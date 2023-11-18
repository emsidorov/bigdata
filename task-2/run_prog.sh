mvn -X clean package
/opt/hadoop-3.3.6/bin/hadoop jar target/mm.jar mm -conf ../tests/test5/config.xml ../tests/test5/A ../tests/test5/B ../results/test5/