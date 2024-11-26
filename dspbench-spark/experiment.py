import os
import time
import datetime;

exec = "stream"

def change_prop(run, app, stage1=1, stage2=1, stage3=1, stage4=1, stage5=1, stage6=1, stage7=1, stage8=1, stage9=1, stage10=1, stage11=1, stage12=1, stage13=1):
    print("change prop")
    if app == "wordcount":
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/wordcount.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/wordcount.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/wordcount.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/wordcount.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/wordcount.properties")
        #Add lines
        os.system('echo "wc.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/wordcount.properties')
        os.system('echo "wc.splitter.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/wordcount.properties')
        os.system('echo "wc.pair_counter.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/wordcount.properties')
        os.system('echo "wc.sink.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/wordcount.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/WC'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/wordcount.properties')

    elif app == "spikedetection":
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/spikedetection.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/spikedetection.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/spikedetection.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/spikedetection.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/spikedetection.properties")
        #Add lines
        os.system('echo "sd.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/spikedetection.properties')
        os.system('echo "sd.moving_average.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/spikedetection.properties')
        os.system('echo "sd.spike_detector.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/spikedetection.properties')
        os.system('echo "sd.sink.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/spikedetection.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/SD'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/spikedetection.properties')

    elif app == "logprocessing":
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/logprocessing.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/logprocessing.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/logprocessing.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/logprocessing.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/logprocessing.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/logprocessing.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/logprocessing.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/logprocessing.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/logprocessing.properties")
        #Add lines
        os.system('echo "lp.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/logprocessing.properties')
        os.system('echo "lp.volume_counter.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/logprocessing.properties')
        os.system('echo "lp.status_counter.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/logprocessing.properties')
        os.system('echo "lp.geo_finder.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/logprocessing.properties')
        os.system('echo "lp.geo_stats.threads='+str(stage5)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/logprocessing.properties')
        os.system('echo "lp.count.sink.threads='+str(stage6)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/logprocessing.properties')
        os.system('echo "lp.status.sink.threads='+str(stage7)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/logprocessing.properties')
        os.system('echo "lp.country.sink.threads='+str(stage8)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/logprocessing.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/LP'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+str(stage5)+str(stage6)+str(stage7)+str(stage8)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/logprocessing.properties')

    elif app == "clickanalytics":
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/clickanalytics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/clickanalytics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/clickanalytics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/clickanalytics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/clickanalytics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/clickanalytics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/clickanalytics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/clickanalytics.properties")
        #Add lines
        os.system('echo "ca.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/clickanalytics.properties')
        os.system('echo "ca.repeats.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/clickanalytics.properties')
        os.system('echo "ca.total_stats.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/clickanalytics.properties')
        os.system('echo "ca.geography.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/clickanalytics.properties')
        os.system('echo "ca.geo_stats.threads='+str(stage5)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/clickanalytics.properties')
        os.system('echo "ca.visit.sink.threads='+str(stage6)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/clickanalytics.properties')
        os.system('echo "ca.location.sink.threads='+str(stage7)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/clickanalytics.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/CA'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+str(stage5)+str(stage6)+str(stage7)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/clickanalytics.properties')

    elif app == "sentimentanalysis":
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/sentimentanalysis.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/sentimentanalysis.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/sentimentanalysis.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/sentimentanalysis.properties")
        #Add lines
        os.system('echo "sa.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/sentimentanalysis.properties')
        os.system('echo "sa.classifier.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/sentimentanalysis.properties')
        os.system('echo "sa.sink.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/sentimentanalysis.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/SA'+str(stage1)+str(stage2)+str(stage3)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/sentimentanalysis.properties')
    
    elif app == "frauddetection":
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/frauddetection.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/frauddetection.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/frauddetection.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/frauddetection.properties")
        #Add lines
        os.system('echo "fd.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/frauddetection.properties')
        os.system('echo "fd.predictor.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/frauddetection.properties')
        os.system('echo "fd.sink.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/frauddetection.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/FD'+str(stage1)+str(stage2)+str(stage3)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/frauddetection.properties')
    
    elif app == "machineoutlier":
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/machineoutlier.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/machineoutlier.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/machineoutlier.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/machineoutlier.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/machineoutlier.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/machineoutlier.properties")
        #Add lines
        os.system('echo "mo.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/machineoutlier.properties')
        os.system('echo "mo.scorer.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/machineoutlier.properties')
        os.system('echo "mo.anomaly_scorer.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/machineoutlier.properties')
        os.system('echo "mo.alert_trigger.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/machineoutlier.properties')
        os.system('echo "mo.sink.threads='+str(stage5)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/machineoutlier.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/MO'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+str(stage5)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/machineoutlier.properties')
    
    elif app == "trafficmonitoring":  
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/trafficmonitoring.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/trafficmonitoring.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/trafficmonitoring.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/trafficmonitoring.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/trafficmonitoring.properties")
        #Add lines
        os.system('echo "tm.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/trafficmonitoring.properties')
        os.system('echo "tm.map_matcher.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/trafficmonitoring.properties')
        os.system('echo "tm.speed_calculator.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/trafficmonitoring.properties')
        os.system('echo "tm.sink.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/trafficmonitoring.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/TM'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/trafficmonitoring.properties')
    
    elif app == "trendingtopics": 
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/trendingtopics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/trendingtopics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/trendingtopics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/trendingtopics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/trendingtopics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/trendingtopics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/trendingtopics.properties")
        #Add lines
        os.system('echo "tt.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/trendingtopics.properties')
        os.system('echo "tt.topic_extractor.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/trendingtopics.properties')
        os.system('echo "tt.counter.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/trendingtopics.properties')
        os.system('echo "tt.iranker.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/trendingtopics.properties')
        os.system('echo "tt.tranker.threads='+str(stage5)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/trendingtopics.properties')
        os.system('echo "tt.sink.threads='+str(stage6)+'" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/trendingtopics.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/TT'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+str(stage5)+str(stage6)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/trendingtopics.properties')
    
def start_job(app):
    print("start job")
    os.system('./bin/dspbench-spark-cluster.sh /home/gmap/DSPBench/dspbench-spark/build/libs/dspbench-spark-uber-1.0.jar ' + app + ' /home/gmap/DSPBench/dspbench-spark/src/main/resources/config/' + app + '.properties')


def time_txt(app, conf, init_time, end_time):
    print("####################################################################")
    with open('/home/gmap/DSPBench/dspbench-spark/txts/'+app+'-'+exec+'-'+str(conf)+'.txt', 'a') as f:
        f.write(str(init_time) + " - " + str(end_time) + "\n")


for i in range(1,3):

    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "clickanalytics", 1, 1, 1, 1, 1, 1, 1)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("clickanalytics")
    end_time = datetime.datetime.now()
    time_txt("clickanalytics", 1111111, init_time, end_time)

    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "clickanalytics", 1, 2, 2, 2, 2, 2, 2)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("clickanalytics")
    end_time = datetime.datetime.now()
    time_txt("clickanalytics", 1222222, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "clickanalytics", 1, 2, 2, 2, 2, 4, 4)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("clickanalytics")
    end_time = datetime.datetime.now()
    time_txt("clickanalytics", 1222244, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "clickanalytics", 1, 2, 4, 2, 4, 4, 4)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("clickanalytics")
    end_time = datetime.datetime.now()
    time_txt("clickanalytics", 1242444, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "clickanalytics", 1, 2, 4, 2, 4, 8, 8)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("clickanalytics")
    end_time = datetime.datetime.now()
    time_txt("clickanalytics", 1242488, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "clickanalytics", 1, 3, 3, 3, 3, 3, 3)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("clickanalytics")
    end_time = datetime.datetime.now()
    time_txt("clickanalytics", 1333333, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "clickanalytics", 1, 3, 3, 3, 3, 6, 6)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("clickanalytics")
    end_time = datetime.datetime.now()
    time_txt("clickanalytics", 1333366, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "clickanalytics", 1, 3, 6, 3, 6, 6, 6)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("clickanalytics")
    end_time = datetime.datetime.now()
    time_txt("clickanalytics", 1363666, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "clickanalytics", 1, 3, 6, 3, 6, 9, 9)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("clickanalytics")
    end_time = datetime.datetime.now()
    time_txt("clickanalytics", 1363699, init_time, end_time)
    #######################################################################################
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "frauddetection", 1, 1, 1)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("frauddetection")
    end_time = datetime.datetime.now()
    time_txt("frauddetection", 111, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "frauddetection", 1, 2, 2)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("frauddetection")
    end_time = datetime.datetime.now()
    time_txt("frauddetection", 122, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "frauddetection", 1, 2, 4)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("frauddetection")
    end_time = datetime.datetime.now()
    time_txt("frauddetection", 124, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "frauddetection", 1, 3, 3)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("frauddetection")
    end_time = datetime.datetime.now()
    time_txt("frauddetection", 133, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "frauddetection", 1, 3, 6)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("frauddetection")
    end_time = datetime.datetime.now()
    time_txt("frauddetection", 136, init_time, end_time)
    #######################################################################################
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "logprocessing", 1, 1, 1, 1, 1, 1, 1, 1)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("logprocessing")
    end_time = datetime.datetime.now()
    time_txt("logprocessing", 11111111, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "logprocessing", 1, 2, 2, 2, 2, 2, 2, 2)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("logprocessing")
    end_time = datetime.datetime.now()
    time_txt("logprocessing", 12222222, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "logprocessing", 1, 2, 2, 2, 4, 4, 4, 4)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("logprocessing")
    end_time = datetime.datetime.now()
    time_txt("logprocessing", 12224444, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "logprocessing", 1, 2, 2, 2, 4, 4, 4, 8)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("logprocessing")
    end_time = datetime.datetime.now()
    time_txt("logprocessing", 12224448, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "logprocessing", 1, 3, 3, 3, 3, 3, 3, 3)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("logprocessing")
    end_time = datetime.datetime.now()
    time_txt("logprocessing", 13333333, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "logprocessing", 1, 3, 3, 3, 6, 6, 6, 6)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("logprocessing")
    end_time = datetime.datetime.now()
    time_txt("logprocessing", 13336666, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "logprocessing", 1, 3, 3, 3, 6, 6, 6, 9)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("logprocessing")
    end_time = datetime.datetime.now()
    time_txt("logprocessing", 13336669, init_time, end_time)
    #######################################################################################
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "machineoutlier", 1, 1, 1, 1, 1)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("machineoutlier")
    end_time = datetime.datetime.now()
    time_txt("machineoutlier", 11111, init_time, end_time)

    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "machineoutlier", 1, 1, 1, 1, 2)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("machineoutlier")
    end_time = datetime.datetime.now()
    time_txt("machineoutlier", 11112, init_time, end_time)

    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "machineoutlier", 1, 1, 1, 1, 3)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("machineoutlier")
    end_time = datetime.datetime.now()
    time_txt("machineoutlier", 11113, init_time, end_time)
    #######################################################################################
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "spikedetection", 1, 1, 1, 1)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spikedetection")
    end_time = datetime.datetime.now()
    time_txt("spikedetection", 1111, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "spikedetection", 1, 2, 2, 2)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spikedetection")
    end_time = datetime.datetime.now()
    time_txt("spikedetection", 1222, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "spikedetection", 1, 2, 4, 4)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spikedetection")
    end_time = datetime.datetime.now()
    time_txt("spikedetection", 1244, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "spikedetection", 1, 2, 4, 8)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spikedetection")
    end_time = datetime.datetime.now()
    time_txt("spikedetection", 1248, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "spikedetection", 1, 3, 3, 3)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spikedetection")
    end_time = datetime.datetime.now()
    time_txt("spikedetection", 1333, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "spikedetection", 1, 3, 6, 6)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spikedetection")
    end_time = datetime.datetime.now()
    time_txt("spikedetection", 1366, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "spikedetection", 1, 3, 6, 9)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spikedetection")
    end_time = datetime.datetime.now()
    time_txt("spikedetection", 1369, init_time, end_time)
    #######################################################################################
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "trafficmonitoring", 1, 1, 1, 1)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trafficmonitoring")
    end_time = datetime.datetime.now()
    time_txt("trafficmonitoring", 1111, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "trafficmonitoring", 1, 2, 2, 2)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trafficmonitoring")
    end_time = datetime.datetime.now()
    time_txt("trafficmonitoring", 1222, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "trafficmonitoring", 1, 2, 4, 4)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trafficmonitoring")
    end_time = datetime.datetime.now()
    time_txt("trafficmonitoring", 1244, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "trafficmonitoring", 1, 2, 4, 8)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trafficmonitoring")
    end_time = datetime.datetime.now()
    time_txt("trafficmonitoring", 1248, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "trafficmonitoring", 1, 3, 3, 3)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trafficmonitoring")
    end_time = datetime.datetime.now()
    time_txt("trafficmonitoring", 1333, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "trafficmonitoring", 1, 3, 6, 6)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trafficmonitoring")
    end_time = datetime.datetime.now()
    time_txt("trafficmonitoring", 1366, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "trafficmonitoring", 1, 3, 6, 9)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trafficmonitoring")
    end_time = datetime.datetime.now()
    time_txt("trafficmonitoring", 1369, init_time, end_time)
    #######################################################################################
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "sentimentanalysis", 1, 1, 1)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("sentimentanalysis")
    end_time = datetime.datetime.now()
    time_txt("sentimentanalysis", 111, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "sentimentanalysis", 1, 2, 2)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("sentimentanalysis")
    end_time = datetime.datetime.now()
    time_txt("sentimentanalysis", 122, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "sentimentanalysis", 1, 2, 4)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("sentimentanalysis")
    end_time = datetime.datetime.now()
    time_txt("sentimentanalysis", 124, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "sentimentanalysis", 1, 3, 3)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("sentimentanalysis")
    end_time = datetime.datetime.now()
    time_txt("sentimentanalysis", 133, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "sentimentanalysis", 1, 3, 6)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("sentimentanalysis")
    end_time = datetime.datetime.now()
    time_txt("sentimentanalysis", 136, init_time, end_time)
    #######################################################################################
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "trendingtopics", 1, 1, 1, 1, 1, 1)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trendingtopics")
    end_time = datetime.datetime.now()
    time_txt("trendingtopics", 111111, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "trendingtopics", 1, 2, 2, 2, 1, 2)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trendingtopics")
    end_time = datetime.datetime.now()
    time_txt("trendingtopics", 122212, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "trendingtopics", 1, 2, 2, 2, 1, 4)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trendingtopics")
    end_time = datetime.datetime.now()
    time_txt("trendingtopics", 122214, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "trendingtopics", 1, 4, 2, 2, 1, 4)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trendingtopics")
    end_time = datetime.datetime.now()
    time_txt("trendingtopics", 142214, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "trendingtopics", 1, 3, 3, 3, 1, 3)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trendingtopics")
    end_time = datetime.datetime.now()
    time_txt("trendingtopics", 133313, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "trendingtopics", 1, 3, 3, 3, 1, 6)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trendingtopics")
    end_time = datetime.datetime.now()
    time_txt("trendingtopics", 133316, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "trendingtopics", 1, 6, 3, 3, 1, 6)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trendingtopics")
    end_time = datetime.datetime.now()
    time_txt("trendingtopics", 163316, init_time, end_time)
    #######################################################################################
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "wordcount", 1, 1, 1, 1)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("wordcount")
    end_time = datetime.datetime.now()
    time_txt("wordcount", 1111, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "wordcount", 1, 2, 2, 2)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("wordcount")
    end_time = datetime.datetime.now()
    time_txt("wordcount", 1222, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "wordcount", 1, 2, 4, 4)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("wordcount")
    end_time = datetime.datetime.now()
    time_txt("wordcount", 1244, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "wordcount", 1, 2, 4, 8)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("wordcount")
    end_time = datetime.datetime.now()
    time_txt("wordcount", 1248, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "wordcount", 1, 3, 3, 3)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("wordcount")
    end_time = datetime.datetime.now()
    time_txt("wordcount", 1333, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "wordcount", 1, 3, 6, 6)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("wordcount")
    end_time = datetime.datetime.now()
    time_txt("wordcount", 1366, init_time, end_time)
    
    time.sleep(20)
    #Change Conf on .properties
    change_prop(i, "wordcount", 1, 3, 6, 9)
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("wordcount")
    end_time = datetime.datetime.now()
    time_txt("wordcount", 1369, init_time, end_time)
    start_job("wordcount")
    end_time = datetime.datetime.now()
    time_txt("wordcount", 1111, init_time, end_time)
