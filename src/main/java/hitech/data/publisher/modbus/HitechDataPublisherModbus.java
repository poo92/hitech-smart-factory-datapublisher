/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hitech.data.publisher.modbus;

import com.intelligt.modbus.jlibmodbus.Modbus;
import com.intelligt.modbus.jlibmodbus.exception.ModbusIOException;
import com.intelligt.modbus.jlibmodbus.exception.ModbusNumberException;
import com.intelligt.modbus.jlibmodbus.exception.ModbusProtocolException;
import com.intelligt.modbus.jlibmodbus.master.ModbusMaster;
import com.intelligt.modbus.jlibmodbus.master.ModbusMasterFactory;
import com.intelligt.modbus.jlibmodbus.serial.SerialParameters;
import com.intelligt.modbus.jlibmodbus.serial.SerialPort;
import static hitech.data.publisher.modbus.ModbusAgentConstants.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import jssc.SerialPortList;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

/**
 *
 * @author Poornima
 */
public class HitechDataPublisherModbus {

    private static Properties agentProperties;
    private static Properties dataMappingProperties;

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        Modbus.setLogLevel(Modbus.LogLevel.LEVEL_DEBUG);

        // setiing up influx client
        // todo get data from a file
        InfluxDB influxDB = InfluxDBFactory.connect("http://35.196.127.106:8086", "hitech", "hitech");
        String dbName = "Hitech";
        influxDB.setDatabase(dbName);

        BatchPoints batchPoints = BatchPoints
                                        .database(dbName)
                                        .tag("async", "true")
                                        .consistency(ConsistencyLevel.ALL)
                                        .build();

        SerialParameters sp = new SerialParameters();
        int modbusSlaveId;
        // Load modbus agent properties from file
        try {
            FileInputStream fis = new FileInputStream("conf/data-publisher.conf.properties");
            agentProperties = new Properties();
            agentProperties.load(fis);
            //TODO validate required parameters
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        ModbusMaster m = null;

        // Set serial communication parameters
        try {
            // Set port
            if (agentProperties.get(MODBUS_COMMUNICATION_PORT_TAG) != null && agentProperties.get(MODBUS_COMMUNICATION_PORT_TAG) instanceof String) {
                sp.setDevice((String) agentProperties.get(MODBUS_COMMUNICATION_PORT_TAG));
            } else {
                // if port is not set in the properties file get the first port from the list
                String[] dev_list = SerialPortList.getPortNames();
                // if there is at least one serial port at your system
                if (dev_list.length > 0) {
                    // you can choose the one of those you need
                    sp.setDevice(dev_list[0]);
                } else {
                    // log error could not find a port value
                    System.exit(0);
                }
            }
            // Set baud rate
            if (agentProperties.get(MODBUS_COMMUNICATION_BAUD_RATE) != null && agentProperties.get(MODBUS_COMMUNICATION_BAUD_RATE) instanceof String) {
                sp.setBaudRate(SerialPort.BaudRate.getBaudRate(Integer.parseInt((String) agentProperties.get(MODBUS_COMMUNICATION_BAUD_RATE))));
            } else {
                sp.setBaudRate(SerialPort.BaudRate.BAUD_RATE_19200);
            }

            // Set data bits
            if (agentProperties.get(MODBUS_COMMUNICATION_DATA_BITS_TAG) != null && agentProperties.get(MODBUS_COMMUNICATION_DATA_BITS_TAG) instanceof String) {
                sp.setDataBits(Integer.parseInt((String) agentProperties.get(MODBUS_COMMUNICATION_BAUD_RATE)));
            } else {
                sp.setDataBits(8);
            }

            // Set parity
            if (agentProperties.get(MODBUS_COMMUNICATION_PARITY_TAG) != null && agentProperties.get(MODBUS_COMMUNICATION_PARITY_TAG) instanceof String) {
                sp.setParity(getParity((String) agentProperties.get(MODBUS_COMMUNICATION_PARITY_TAG)));
            } else {
                sp.setParity(SerialPort.Parity.EVEN);
            }

            // Set Stop bits
            if (agentProperties.get(MODBUS_COMMUNICATION_STOP_BIT_TAG) != null && agentProperties.get(MODBUS_COMMUNICATION_STOP_BIT_TAG) instanceof String) {
                sp.setStopBits(Integer.parseInt((String) agentProperties.get(MODBUS_COMMUNICATION_STOP_BIT_TAG)));
            } else {
                sp.setStopBits(1);
            }

            // Set Slave ID
            if (agentProperties.get(MODBUS_COMMUNICATION_PLC_SLAVE_ID) != null && agentProperties.get(MODBUS_COMMUNICATION_PLC_SLAVE_ID) instanceof String) {
                modbusSlaveId = Integer.parseInt((String) agentProperties.get(MODBUS_COMMUNICATION_PLC_SLAVE_ID));
            } else {
                modbusSlaveId = 1;
            }

            //Load data mapping properties
            try {
                FileInputStream fis = new FileInputStream("conf/data-mapping.conf.properties");
                dataMappingProperties = new Properties();
                dataMappingProperties.load(fis);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            // Get slave IDs
            String slaveIds;
            if (dataMappingProperties.get(SLAVE_IDS) != null && dataMappingProperties.get(SLAVE_IDS) instanceof String) {
                slaveIds = (String) dataMappingProperties.get(SLAVE_IDS);
            } else {
                slaveIds = "1";
            }
            String[] slaveIdArray = slaveIds.split(",");
            // Instantiate maps for each modbus slave
            Map<String, HashMap> slaveMaps = new HashMap<String, HashMap>();
            for (String aSlaveIdArray : slaveIdArray) {
                slaveMaps.put(aSlaveIdArray.trim(), new HashMap<String, String>());
            }
            // populate maps
            for (Entry<Object, Object> keyVal : dataMappingProperties.entrySet()) {
                String key = (String) keyVal.getKey();
                String val = (String) keyVal.getValue();
                if (!key.startsWith("slave")) {
                    String[] splitKey = key.split("-");
                    slaveMaps.get(splitKey[0].trim()).put(splitKey[1].trim(), val.trim());
                }
            }

            // DS to hold previous values of each sensor
            HashMap<String, Integer> previousValues = new HashMap<>();

            m = ModbusMasterFactory.createModbusMasterRTU(sp);
            m.setResponseTimeout(500);
            m.connect();

            // push data
            while(true) {
                for (String slaveId : slaveIdArray) {
                    HashMap<String, String> kayValMap;
                    kayValMap = slaveMaps.get(slaveId.trim());
                    for (Entry<String, String> entry : kayValMap.entrySet()) {
                        String key = entry.getKey();
                        String tagValue = entry.getValue();


                        try {
                            // at next string we receive ten registers from a slave with id of 1 at offset of 0.
                            int[] registerValues = m.readHoldingRegisters(modbusSlaveId, getOffset(key), 1);
                            // print values
                            for (int value : registerValues) {
                                // check with previous values
                                String completeKey = slaveId + "-" + key;
                                if (previousValues.containsKey(completeKey)) {
                                    if (value != previousValues.get(completeKey)) {
                                        System.out.println("Address: " + key + ", Value: " + value);
                                        Point point1 = Point.measurement(tagValue)
                                                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                                                .addField("value", value)
                                                .build();

                                        batchPoints.point(point1);
                                        influxDB.write(batchPoints);

                                        // update previous value map
                                        previousValues.put(completeKey, value);
                                    }
                                } else {
                                    System.out.println("Address: " + key + ", Value: " + value);
                                    Point point1 = Point.measurement(tagValue)
                                            .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                                            .addField("value", value)
                                            .build();

                                    batchPoints.point(point1);
                                    influxDB.write(batchPoints);

                                    // update previous value map
                                    previousValues.put(completeKey, value);
                                }

                            }
                        } catch (RuntimeException | ModbusIOException | ModbusProtocolException | ModbusNumberException e) {
                            System.out.println("Error");
//                            e.printStackTrace();
                        }
                    }
                }
//                TimeUnit.SECONDS.sleep(1);
            }

        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                m.disconnect();
                System.out.println("Connection closed");
            } catch (ModbusIOException e1) {
                e1.printStackTrace();
            }
        }
    }

    private static SerialPort.Parity getParity(String parity) {
        SerialPort.Parity parityVal;
        if (parity.equalsIgnoreCase("even")) {
            parityVal = SerialPort.Parity.EVEN;
        } else if (parity.equalsIgnoreCase("none")) {
            parityVal = SerialPort.Parity.NONE;
        } else if (parity.equalsIgnoreCase("odd")) {
            parityVal = SerialPort.Parity.ODD;
        } else {
            parityVal = SerialPort.Parity.EVEN;
        }
        return parityVal;
    }

    private static int getOffset(String key) {
        int offset = 0;
        if (key.contains("fd")) {
            String keyVal = key.replace("fd", "");
            // TODO 18432 can be changed from PLC to PLC. Need to get from properties file
            offset = 18432 + Integer.parseInt(keyVal);
        } else if (key.contains("d")) {
            String keyVal = key.replace("d", "");
            // TODO 0 can be changed from PLC to PLC. Need to get from properties file
            offset = 0 + Integer.parseInt(keyVal);
        }
        return offset;
    }

}
