import os
import time
import datetime;

exec = "stream"

def change_prop(run, app, stage1=1, stage2=1, stage3=1, stage4=1, stage5=1, stage6=1, stage7=1, stage8=1, stage9=1, stage10=1, stage11=1, stage12=1, stage13=1):
    print("change prop")
    if app == "wordcount":
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/wordcount.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/wordcount.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/wordcount.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/wordcount.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/wordcount.properties")
        #Add lines
        os.system('echo "wc.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/wordcount.properties')
        os.system('echo "wc.splitter.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/wordcount.properties')
        os.system('echo "wc.counter.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/wordcount.properties')
        os.system('echo "wc.sink.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/wordcount.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/WC'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/wordcount.properties')

    elif app == "spikedetection":
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spikedetection.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spikedetection.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spikedetection.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spikedetection.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spikedetection.properties")
        #Add lines
        os.system('echo "sd.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spikedetection.properties')
        os.system('echo "sd.moving_average.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spikedetection.properties')
        os.system('echo "sd.spike_detector.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spikedetection.properties')
        os.system('echo "sd.sink.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spikedetection.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/SD'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spikedetection.properties')

    elif app == "logprocessing":
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties")
        #Add lines
        os.system('echo "lp.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties')
        os.system('echo "lp.volume_counter.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties')
        os.system('echo "lp.status_counter.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties')
        os.system('echo "lp.geo_finder.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties')
        os.system('echo "lp.geo_stats.threads='+str(stage5)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties')
        os.system('echo "lp.count.sink.threads='+str(stage6)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties')
        os.system('echo "lp.status.sink.threads='+str(stage7)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties')
        os.system('echo "lp.country.sink.threads='+str(stage8)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/LP'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+str(stage5)+str(stage6)+str(stage7)+str(stage8)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/logprocessing.properties')

    elif app == "clickanalytics":
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/clickanalytics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/clickanalytics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/clickanalytics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/clickanalytics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/clickanalytics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/clickanalytics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/clickanalytics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/clickanalytics.properties")
        #Add lines
        os.system('echo "ca.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/clickanalytics.properties')
        os.system('echo "ca.repeats.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/clickanalytics.properties')
        os.system('echo "ca.total_stats.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/clickanalytics.properties')
        os.system('echo "ca.geography.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/clickanalytics.properties')
        os.system('echo "ca.geo_stats.threads='+str(stage5)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/clickanalytics.properties')
        os.system('echo "ca.visit.sink.threads='+str(stage6)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/clickanalytics.properties')
        os.system('echo "ca.location.sink.threads='+str(stage7)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/clickanalytics.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/CA'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+str(stage5)+str(stage6)+str(stage7)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/clickanalytics.properties')

    elif app == "sentimentanalysis":
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/sentimentanalysis.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/sentimentanalysis.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/sentimentanalysis.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/sentimentanalysis.properties")
        #Add lines
        os.system('echo "sa.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/sentimentanalysis.properties')
        os.system('echo "sa.classifier.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/sentimentanalysis.properties')
        os.system('echo "sa.sink.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/sentimentanalysis.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/SA'+str(stage1)+str(stage2)+str(stage3)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/sentimentanalysis.properties')
    
    elif app == "frauddetection":
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/frauddetection.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/frauddetection.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/frauddetection.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/frauddetection.properties")
        #Add lines
        os.system('echo "fd.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/frauddetection.properties')
        os.system('echo "fd.predictor.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/frauddetection.properties')
        os.system('echo "fd.sink.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/frauddetection.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/FD'+str(stage1)+str(stage2)+str(stage3)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/frauddetection.properties')
    
    elif app == "machineoutlier":
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/machineoutlier.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/machineoutlier.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/machineoutlier.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/machineoutlier.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/machineoutlier.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/machineoutlier.properties")
        #Add lines
        os.system('echo "mo.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/machineoutlier.properties')
        os.system('echo "mo.scorer.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/machineoutlier.properties')
        os.system('echo "mo.anomaly_scorer.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/machineoutlier.properties')
        os.system('echo "mo.alert_trigger.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/machineoutlier.properties')
        os.system('echo "mo.sink.threads='+str(stage5)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/machineoutlier.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/MO'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+str(stage5)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/machineoutlier.properties')
    
    elif app == "smartgrid":
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties")
        #Add lines
        os.system('echo "sg.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties')
        os.system('echo "sg.sliding_window.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties')
        os.system('echo "sg.global_median.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties')
        os.system('echo "sg.plug_median.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties')
        os.system('echo "sg.outlier_detector.threads='+str(stage5)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties')
        os.system('echo "sg.house_load.threads='+str(stage6)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties')
        os.system('echo "sg.plug_load.threads='+str(stage7)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties')
        os.system('echo "sg.outlier.sink.threads='+str(stage8)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties')
        os.system('echo "sg.prediction.sink.threads='+str(stage9)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/SG'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+str(stage5)+str(stage6)+str(stage7)+str(stage8)+str(stage9)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/smartgrid.properties')
    
    elif app == "trafficmonitoring":  
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trafficmonitoring.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trafficmonitoring.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trafficmonitoring.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trafficmonitoring.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trafficmonitoring.properties")
        #Add lines
        os.system('echo "tm.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trafficmonitoring.properties')
        os.system('echo "tm.map_matcher.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trafficmonitoring.properties')
        os.system('echo "tm.speed_calculator.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trafficmonitoring.properties')
        os.system('echo "tm.sink.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trafficmonitoring.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/TM'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trafficmonitoring.properties')
    
    elif app == "adanalytics": 
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/adanalytics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/adanalytics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/adanalytics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/adanalytics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/adanalytics.properties")
        #Add lines
        os.system('echo "aa.click.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/adanalytics.properties')
        os.system('echo "aa.impressions.parser.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/adanalytics.properties')
        os.system('echo "aa.ctr.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/adanalytics.properties')
        os.system('echo "aa.sink.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/adanalytics.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/AA'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/adanalytics.properties')
    
    elif app == "bargainindex": 
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/bargainindex.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/bargainindex.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/bargainindex.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/bargainindex.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/bargainindex.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/bargainindex.properties")
        #Add lines
        os.system('echo "bi.quotes.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/bargainindex.properties')
        os.system('echo "bi.trades.parser.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/bargainindex.properties')
        os.system('echo "bi.vwap.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/bargainindex.properties')
        os.system('echo "bi.bargainindex.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/bargainindex.properties')
        os.system('echo "bi.sink.threads='+str(stage5)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/bargainindex.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/BI'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+str(stage5)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/bargainindex.properties')
    
    elif app == "reinforcementlearner": 
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/reinforcementlearner.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/reinforcementlearner.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/reinforcementlearner.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/reinforcementlearner.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/reinforcementlearner.properties")
        #Add lines
        os.system('echo "rl.event.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/reinforcementlearner.properties')
        os.system('echo "rl.reward.parser.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/reinforcementlearner.properties')
        os.system('echo "rl.learner.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/reinforcementlearner.properties')
        os.system('echo "rl.sink.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/reinforcementlearner.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/RL'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/reinforcementlearner.properties')
    
    elif app == "spamfilter": 
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spamfilter.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spamfilter.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spamfilter.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spamfilter.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spamfilter.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spamfilter.properties")
        #Add lines
        os.system('echo "sf.training.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spamfilter.properties')
        os.system('echo "sf.analysis.parser.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spamfilter.properties')
        os.system('echo "sf.tokenizer.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spamfilter.properties')
        os.system('echo "sf.bayesrule.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spamfilter.properties')
        os.system('echo "sf.sink.threads='+str(stage5)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spamfilter.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/SF'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+str(stage5)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/spamfilter.properties')
    
    elif app == "trendingtopics": 
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trendingtopics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trendingtopics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trendingtopics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trendingtopics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trendingtopics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trendingtopics.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trendingtopics.properties")
        #Add lines
        os.system('echo "tt.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trendingtopics.properties')
        os.system('echo "tt.topic_extractor.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trendingtopics.properties')
        os.system('echo "tt.counter.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trendingtopics.properties')
        os.system('echo "tt.iranker.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trendingtopics.properties')
        os.system('echo "tt.tranker.threads='+str(stage5)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trendingtopics.properties')
        os.system('echo "tt.sink.threads='+str(stage6)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trendingtopics.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/TT'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+str(stage5)+str(stage6)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/trendingtopics.properties')
    
    elif app == "voipstream": 
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties")
        #Add lines
        os.system('echo "vs.parser.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties')
        os.system('echo "vs.vardetect.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties')
        os.system('echo "vs.encr.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties')
        os.system('echo "vs.ecr.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties')
        os.system('echo "vs.rcr.threads='+str(stage5)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties')
        os.system('echo "vs.ecr24.threads='+str(stage6)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties')
        os.system('echo "vs.ct24.threads='+str(stage7)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties')
        os.system('echo "vs.globalacd.threads='+str(stage8)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties')
        os.system('echo "vs.fofir.threads='+str(stage9)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties')
        os.system('echo "vs.url.threads='+str(stage10)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties')
        os.system('echo "vs.acd.threads='+str(stage11)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties')
        os.system('echo "vs.scorer.threads='+str(stage12)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties')
        os.system('echo "vs.sink.threads='+str(stage13)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/VS'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+str(stage5)+str(stage6)+str(stage7)+str(stage8)+str(stage9)+str(stage10)+str(stage11)+str(stage12)+str(stage13)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/voipstream.properties')
    
    elif app == "YSB":
        #Remove last line
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/YSB.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/YSB.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/YSB.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/YSB.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/YSB.properties")
        os.system("sed -i '$ d' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/YSB.properties")
        #Add lines
        os.system('echo "ysb.source.threads='+str(stage1)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/YSB.properties')
        os.system('echo "ysb.filter.threads='+str(stage2)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/YSB.properties')
        os.system('echo "ysb.joiner.threads='+str(stage3)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/YSB.properties')
        os.system('echo "ysb.aggregator.threads='+str(stage4)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/YSB.properties')
        os.system('echo "ysb.sink.threads='+str(stage5)+'" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/YSB.properties')
        os.system('echo "metrics.output=/home/gmap/metrics/'+exec+'/YSB'+str(stage1)+str(stage2)+str(stage3)+str(stage4)+str(stage5)+'/'+str(run)+'/" >> /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/YSB.properties')

def start_job(app):
    print("start job")
    # ./bin/dspbench-flink-cluster.sh /home/DSPBench/dspbench-flink/build/libs/dspbench-flink-uber-1.0.jar wordcount /home/DSPBench/dspbench-flink/build/resources/main/config/wordcount.properties
    os.system('./bin/dspbench-flink-cluster.sh /home/gmap/DSPBench/dspbench-flink/build/libs/dspbench-flink-uber-1.0.jar ' + app + ' /home/gmap/DSPBench/dspbench-flink/src/main/resources/config/' + app + '.properties')

def stop_cluster():
    os.system('~/maven/flink-1.18.1/bin/stop-cluster.sh')

def start_cluster():
    os.system('~/maven/flink-1.18.1/bin/start-cluster.sh')

def restart_cluster():
    print("restart cluster")
    stop_cluster() #os.system('~/flink-1.17.1/bin/stop-cluster.sh')
    time.sleep(10)
    start_cluster() #os.system('~/flink-1.17.1/bin/start-cluster.sh')
    time.sleep(30)

def time_txt(app, conf, init_time, end_time):
    print("####################################################################")
    with open('/home/gmap/DSPBench/dspbench-flink/txts/'+app+'-'+exec+'-'+str(conf)+'.txt', 'a') as f:
        f.write(str(init_time) + " - " + str(end_time) + "\n")

start_cluster()

for i in range(1,6):
    
    #Change Confs on .properties
    change_prop(i, "adanalytics", 1, 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("adanalytics")
    end_time = datetime.datetime.now()
    time_txt("adanalytics", 1111, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "adanalytics", 1, 1, 2, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("adanalytics")
    end_time = datetime.datetime.now()
    time_txt("adanalytics", 1122, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "adanalytics", 1, 1, 2, 4)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("adanalytics")
    end_time = datetime.datetime.now()
    time_txt("adanalytics", 1124, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "adanalytics", 1, 1, 3, 3)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("adanalytics")
    end_time = datetime.datetime.now()
    time_txt("adanalytics", 1133, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "adanalytics", 1, 1, 3, 6)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("adanalytics")
    end_time = datetime.datetime.now()
    time_txt("adanalytics", 1136, init_time, end_time)
    
    #######################################################################################
    #Change Confs on .properties
    change_prop(i, "bargainindex", 1, 1, 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("bargainindex")
    end_time = datetime.datetime.now()
    time_txt("bargainindex", 11111, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "bargainindex", 1, 1, 2, 2, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("bargainindex")
    end_time = datetime.datetime.now()
    time_txt("bargainindex", 11222, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "bargainindex", 1, 1, 2, 4, 4)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("bargainindex")
    end_time = datetime.datetime.now()
    time_txt("bargainindex", 11244, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "bargainindex", 1, 1, 2, 4, 8)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("bargainindex")
    end_time = datetime.datetime.now()
    time_txt("bargainindex", 11248, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "bargainindex", 1, 1, 3, 3, 3)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("bargainindex")
    end_time = datetime.datetime.now()
    time_txt("bargainindex", 11333, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "bargainindex", 1, 1, 3, 6, 6)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("bargainindex")
    end_time = datetime.datetime.now()
    time_txt("bargainindex", 11366, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "bargainindex", 1, 1, 3, 6, 9)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("bargainindex")
    end_time = datetime.datetime.now()
    time_txt("bargainindex", 11369, init_time, end_time)
    #######################################################################################
    #Change Confs on .properties
    change_prop(i, "clickanalytics", 1, 1, 1, 1, 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("clickanalytics")
    end_time = datetime.datetime.now()
    time_txt("clickanalytics", 1111111, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "clickanalytics", 1, 2, 2, 2, 2, 2, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("clickanalytics")
    end_time = datetime.datetime.now()
    time_txt("clickanalytics", 1222222, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "clickanalytics", 1, 2, 2, 2, 2, 4, 4)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("clickanalytics")
    end_time = datetime.datetime.now()
    time_txt("clickanalytics", 1222244, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "clickanalytics", 1, 2, 4, 2, 4, 4, 4)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("clickanalytics")
    end_time = datetime.datetime.now()
    time_txt("clickanalytics", 1242444, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "clickanalytics", 1, 2, 4, 2, 4, 8, 8)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("clickanalytics")
    end_time = datetime.datetime.now()
    time_txt("clickanalytics", 1242488, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "clickanalytics", 1, 3, 3, 3, 3, 3, 3)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("clickanalytics")
    end_time = datetime.datetime.now()
    time_txt("clickanalytics", 1333333, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "clickanalytics", 1, 3, 3, 3, 3, 6, 6)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("clickanalytics")
    end_time = datetime.datetime.now()
    time_txt("clickanalytics", 1333366, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "clickanalytics", 1, 3, 6, 3, 6, 6, 6)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("clickanalytics")
    end_time = datetime.datetime.now()
    time_txt("clickanalytics", 1363666, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "clickanalytics", 1, 3, 6, 3, 6, 9, 9)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("clickanalytics")
    end_time = datetime.datetime.now()
    time_txt("clickanalytics", 1363699, init_time, end_time)
    #######################################################################################
    #Change Confs on .properties
    change_prop(i, "frauddetection", 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("frauddetection")
    end_time = datetime.datetime.now()
    time_txt("frauddetection", 111, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "frauddetection", 1, 2, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("frauddetection")
    end_time = datetime.datetime.now()
    time_txt("frauddetection", 122, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "frauddetection", 1, 2, 4)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("frauddetection")
    end_time = datetime.datetime.now()
    time_txt("frauddetection", 124, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "frauddetection", 1, 3, 3)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("frauddetection")
    end_time = datetime.datetime.now()
    time_txt("frauddetection", 133, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "frauddetection", 1, 3, 6)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("frauddetection")
    end_time = datetime.datetime.now()
    time_txt("frauddetection", 136, init_time, end_time)
    #######################################################################################
    #Change Confs on .properties
    change_prop(i, "logprocessing", 1, 1, 1, 1, 1, 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("logprocessing")
    end_time = datetime.datetime.now()
    time_txt("logprocessing", 11111111, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "logprocessing", 1, 2, 2, 2, 2, 2, 2, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("logprocessing")
    end_time = datetime.datetime.now()
    time_txt("logprocessing", 12222222, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "logprocessing", 1, 2, 2, 2, 4, 4, 4, 4)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("logprocessing")
    end_time = datetime.datetime.now()
    time_txt("logprocessing", 12224444, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "logprocessing", 1, 2, 2, 2, 4, 4, 4, 8)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("logprocessing")
    end_time = datetime.datetime.now()
    time_txt("logprocessing", 12224448, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "logprocessing", 1, 3, 3, 3, 3, 3, 3, 3)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("logprocessing")
    end_time = datetime.datetime.now()
    time_txt("logprocessing", 13333333, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "logprocessing", 1, 3, 3, 3, 6, 6, 6, 6)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("logprocessing")
    end_time = datetime.datetime.now()
    time_txt("logprocessing", 13336666, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "logprocessing", 1, 3, 3, 3, 6, 6, 6, 9)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("logprocessing")
    end_time = datetime.datetime.now()
    time_txt("logprocessing", 13336669, init_time, end_time)
    #######################################################################################
    #Change Confs on .properties
    change_prop(i, "machineoutlier", 1, 1, 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("machineoutlier")
    end_time = datetime.datetime.now()
    time_txt("machineoutlier", 11111, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "machineoutlier", 1, 1, 1, 1, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("machineoutlier")
    end_time = datetime.datetime.now()
    time_txt("machineoutlier", 11112, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "machineoutlier", 1, 1, 1, 1, 3)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("machineoutlier")
    end_time = datetime.datetime.now()
    time_txt("machineoutlier", 11113, init_time, end_time)
    #######################################################################################
    #Change Confs on .properties
    change_prop(i, "reinforcementlearner", 1, 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("reinforcementlearner")
    end_time = datetime.datetime.now()
    time_txt("reinforcementlearner", 1111, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "reinforcementlearner", 1, 1, 2, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("reinforcementlearner")
    end_time = datetime.datetime.now()
    time_txt("reinforcementlearner", 1122, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "reinforcementlearner", 1, 1, 2, 4)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("reinforcementlearner")
    end_time = datetime.datetime.now()
    time_txt("reinforcementlearner", 1124, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "reinforcementlearner", 1, 1, 3, 3)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("reinforcementlearner")
    end_time = datetime.datetime.now()
    time_txt("reinforcementlearner", 1133, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "reinforcementlearner", 1, 1, 3, 6)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("reinforcementlearner")
    end_time = datetime.datetime.now()
    time_txt("reinforcementlearner", 1136, init_time, end_time)
    #######################################################################################
    #Change Confs on .properties
    change_prop(i, "smartgrid", 1, 1, 1, 1, 1, 1, 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("smartgrid")
    end_time = datetime.datetime.now()
    time_txt("smartgrid", 111111111, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "smartgrid", 1, 1, 1, 1, 1, 1, 1, 2, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("smartgrid")
    end_time = datetime.datetime.now()
    time_txt("smartgrid", 111111122, init_time, end_time)

    #Change Confs on .properties
    change_prop(i, "smartgrid", 1, 1, 1, 1, 1, 1, 1, 3, 3)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("smartgrid")
    end_time = datetime.datetime.now()
    time_txt("smartgrid", 111111133, init_time, end_time)
    #######################################################################################
    #Change Confs on .properties
    change_prop(i, "spamfilter", 1, 1, 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spamfilter")
    end_time = datetime.datetime.now()
    time_txt("spamfilter", 11111, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "spamfilter", 1, 1, 2, 2, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spamfilter")
    end_time = datetime.datetime.now()
    time_txt("spamfilter", 11222, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "spamfilter", 1, 1, 2, 4, 4)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spamfilter")
    end_time = datetime.datetime.now()
    time_txt("spamfilter", 11244, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "spamfilter", 1, 1, 2, 4, 8)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spamfilter")
    end_time = datetime.datetime.now()
    time_txt("spamfilter", 11248, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "spamfilter", 1, 1, 3, 3, 3)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spamfilter")
    end_time = datetime.datetime.now()
    time_txt("spamfilter", 11333, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "spamfilter", 1, 1, 3, 6, 6)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spamfilter")
    end_time = datetime.datetime.now()
    time_txt("spamfilter", 11366, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "spamfilter", 1, 1, 3, 6, 9)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spamfilter")
    end_time = datetime.datetime.now()
    time_txt("spamfilter", 11369, init_time, end_time)
    #######################################################################################
    #Change Confs on .properties
    change_prop(i, "spikedetection", 1, 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spikedetection")
    end_time = datetime.datetime.now()
    time_txt("spikedetection", 1111, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "spikedetection", 1, 2, 2, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spikedetection")
    end_time = datetime.datetime.now()
    time_txt("spikedetection", 1222, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "spikedetection", 1, 2, 4, 4)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spikedetection")
    end_time = datetime.datetime.now()
    time_txt("spikedetection", 1244, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "spikedetection", 1, 2, 4, 8)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spikedetection")
    end_time = datetime.datetime.now()
    time_txt("spikedetection", 1248, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "spikedetection", 1, 3, 3, 3)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spikedetection")
    end_time = datetime.datetime.now()
    time_txt("spikedetection", 1333, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "spikedetection", 1, 3, 6, 6)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spikedetection")
    end_time = datetime.datetime.now()
    time_txt("spikedetection", 1366, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "spikedetection", 1, 3, 6, 9)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("spikedetection")
    end_time = datetime.datetime.now()
    time_txt("spikedetection", 1369, init_time, end_time)
    #######################################################################################
    #Change Confs on .properties
    change_prop(i, "trafficmonitoring", 1, 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trafficmonitoring")
    end_time = datetime.datetime.now()
    time_txt("trafficmonitoring", 1111, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "trafficmonitoring", 1, 2, 2, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trafficmonitoring")
    end_time = datetime.datetime.now()
    time_txt("trafficmonitoring", 1222, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "trafficmonitoring", 1, 2, 4, 4)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trafficmonitoring")
    end_time = datetime.datetime.now()
    time_txt("trafficmonitoring", 1244, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "trafficmonitoring", 1, 2, 4, 8)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trafficmonitoring")
    end_time = datetime.datetime.now()
    time_txt("trafficmonitoring", 1248, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "trafficmonitoring", 1, 3, 3, 3)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trafficmonitoring")
    end_time = datetime.datetime.now()
    time_txt("trafficmonitoring", 1333, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "trafficmonitoring", 1, 3, 6, 6)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trafficmonitoring")
    end_time = datetime.datetime.now()
    time_txt("trafficmonitoring", 1366, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "trafficmonitoring", 1, 3, 6, 9)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trafficmonitoring")
    end_time = datetime.datetime.now()
    time_txt("trafficmonitoring", 1369, init_time, end_time)
    #######################################################################################
    #Change Confs on .properties
    change_prop(i, "sentimentanalysis", 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("sentimentanalysis")
    end_time = datetime.datetime.now()
    time_txt("sentimentanalysis", 111, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "sentimentanalysis", 1, 2, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("sentimentanalysis")
    end_time = datetime.datetime.now()
    time_txt("sentimentanalysis", 122, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "sentimentanalysis", 1, 2, 4)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("sentimentanalysis")
    end_time = datetime.datetime.now()
    time_txt("sentimentanalysis", 124, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "sentimentanalysis", 1, 3, 3)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("sentimentanalysis")
    end_time = datetime.datetime.now()
    time_txt("sentimentanalysis", 133, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "sentimentanalysis", 1, 3, 6)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("sentimentanalysis")
    end_time = datetime.datetime.now()
    time_txt("sentimentanalysis", 136, init_time, end_time)
    #######################################################################################
    #Change Confs on .properties
    change_prop(i, "trendingtopics", 1, 1, 1, 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trendingtopics")
    end_time = datetime.datetime.now()
    time_txt("trendingtopics", 111111, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "trendingtopics", 1, 2, 2, 2, 1, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trendingtopics")
    end_time = datetime.datetime.now()
    time_txt("trendingtopics", 122212, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "trendingtopics", 1, 2, 2, 2, 1, 4)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trendingtopics")
    end_time = datetime.datetime.now()
    time_txt("trendingtopics", 122214, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "trendingtopics", 1, 4, 2, 2, 1, 4)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trendingtopics")
    end_time = datetime.datetime.now()
    time_txt("trendingtopics", 142214, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "trendingtopics", 1, 3, 3, 3, 1, 3)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trendingtopics")
    end_time = datetime.datetime.now()
    time_txt("trendingtopics", 133313, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "trendingtopics", 1, 3, 3, 3, 1, 6)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trendingtopics")
    end_time = datetime.datetime.now()
    time_txt("trendingtopics", 133316, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "trendingtopics", 1, 6, 3, 3, 1, 6)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("trendingtopics")
    end_time = datetime.datetime.now()
    time_txt("trendingtopics", 163316, init_time, end_time)
    
    #######################################################################################
    #Change Confs on .properties
    change_prop(i, "voipstream", 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("voipstream")
    end_time = datetime.datetime.now()
    time_txt("voipstream", 1111111111111, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "voipstream", 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("voipstream")
    end_time = datetime.datetime.now()
    time_txt("voipstream", 1222222222222, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "voipstream", 1, 2, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("voipstream")
    end_time = datetime.datetime.now()
    time_txt("voipstream", 1244444444444, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "voipstream", 1, 2, 4, 4, 4, 4, 4, 4, 8, 8, 8, 8, 8)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("voipstream")
    end_time = datetime.datetime.now()
    time_txt("voipstream", 1244444488888, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "voipstream", 1, 2, 4, 4, 4, 4, 4, 4, 8, 8, 8, 12, 12)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("voipstream")
    end_time = datetime.datetime.now()
    time_txt("voipstream", 124444448881212, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "voipstream", 1, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("voipstream")
    end_time = datetime.datetime.now()
    time_txt("voipstream", 1333333333333, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "voipstream", 1, 3, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("voipstream")
    end_time = datetime.datetime.now()
    time_txt("voipstream", 1366666666666, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "voipstream", 1, 3, 6, 6, 6, 6, 6, 6, 9, 9, 9, 9, 9)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("voipstream")
    end_time = datetime.datetime.now()
    time_txt("voipstream", 1366666699999, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "voipstream", 1, 3, 6, 6, 6, 6, 6, 6, 9, 9, 9, 12, 12)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("voipstream")
    end_time = datetime.datetime.now()
    time_txt("voipstream", 136666669991212, init_time, end_time)
    #######################################################################################
    #Change Confs on .properties
    change_prop(i, "wordcount", 1, 1, 1, 1)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("wordcount")
    end_time = datetime.datetime.now()
    time_txt("wordcount", 1111, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "wordcount", 1, 2, 2, 2)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("wordcount")
    end_time = datetime.datetime.now()
    time_txt("wordcount", 1222, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "wordcount", 1, 2, 4, 4)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("wordcount")
    end_time = datetime.datetime.now()
    time_txt("wordcount", 1244, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "wordcount", 1, 2, 4, 8)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("wordcount")
    end_time = datetime.datetime.now()
    time_txt("wordcount", 1248, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "wordcount", 1, 3, 3, 3)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("wordcount")
    end_time = datetime.datetime.now()
    time_txt("wordcount", 1333, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "wordcount", 1, 3, 6, 6)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("wordcount")
    end_time = datetime.datetime.now()
    time_txt("wordcount", 1366, init_time, end_time)
    
    #Change Confs on .properties
    change_prop(i, "wordcount", 1, 3, 6, 9)
    restart_cluster()
    #Gera .txt com tempo de exec
    init_time = datetime.datetime.now()
    start_job("wordcount")
    end_time = datetime.datetime.now()
    time_txt("wordcount", 1369, init_time, end_time)
    
stop_cluster()
