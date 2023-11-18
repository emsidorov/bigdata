mvn -X clean package
/opt/hadoop-3.3.6/bin/hadoop jar target/candle.jar candle -conf ../tests/test5/config.xml \
    ../tests/test5/input.csv ../results/test5/