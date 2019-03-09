import org.sensorhub.impl.SensorHub;


public class TestSensorHub
{
    private TestSensorHub()
    {        
    }
    
    
    public static void main(String[] args)
    {
        SensorHub.main(new String[] {"src/main/resources/config_empty_sost.json", "storage"});
        //SensorHub.main(new String[] {"src/main/resources/config_empty_sost_basicauth.json", "storage"});
    }

}
