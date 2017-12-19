
import com.intelligt.modbus.jlibmodbus.Modbus;
import com.intelligt.modbus.jlibmodbus.exception.ModbusIOException;
import com.intelligt.modbus.jlibmodbus.master.ModbusMaster;
import com.intelligt.modbus.jlibmodbus.master.ModbusMasterFactory;
import com.intelligt.modbus.jlibmodbus.serial.SerialParameters;
import com.intelligt.modbus.jlibmodbus.serial.SerialPort;
import jssc.SerialPortList;

/*
 * Copyright 2017 Poornima.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 *
 * @author Poornima
 */
public class SimpleMasterRTU {
    
     static public void main(String[] arg) {
        SerialParameters sp = new SerialParameters();
        Modbus.setLogLevel(Modbus.LogLevel.LEVEL_DEBUG);
        try {
            // you can use just string to get connection with remote slave,
            // but you can also get a list of all serial ports available at your system.
            String[] dev_list = SerialPortList.getPortNames();
            // if there is at least one serial port at your system
            if (dev_list.length > 0) {
                // you can choose the one of those you need
                sp.setDevice(dev_list[0]);
                // these parameters are set by default
                sp.setBaudRate(SerialPort.BaudRate.BAUD_RATE_19200);
                sp.setDataBits(8);
                sp.setParity(SerialPort.Parity.EVEN);
                sp.setStopBits(1);
                //you can choose the library to use.
                //the library uses jssc by default.
                //
                //first, you should set the factory that will be used by library to create an instance of SerialPort.
                //SerialUtils.setSerialPortFactory(new SerialPortFactoryRXTX());
                //  JSSC is Java Simple Serial Connector
                //SerialUtils.setSerialPortFactory(new SerialPortFactoryJSSC());
                //  PJC is PureJavaComm.
                //SerialUtils.setSerialPortFactory(new SerialPortFactoryPJC());
                //  JavaComm is the Java Communications API (also known as javax.comm)
                //SerialUtils.setSerialPortFactory(new SerialPortFactoryJavaComm());
                //in case of using serial-to-wifi adapter
                //String ip = "192.168.0.180";//for instance
                //int port  = 777;
                //SerialUtils.setSerialPortFactory(new SerialPortFactoryTcp(new TcpParameters(InetAddress.getByName(ip), port, true)));
                // you should use another method:
                //next you just create your master and use it.
                ModbusMaster m = ModbusMasterFactory.createModbusMasterRTU(sp);
                m.connect();

                int slaveId = 1;
                int offset = 0;
                int quantity = 2;
                //you can invoke #connect method manually, otherwise it'll be invoked automatically
                try {
                    // at next string we receive ten registers from a slave with id of 1 at offset of 0.
                    int[] registerValues = m.readHoldingRegisters(slaveId, offset, quantity);
                    // print values
                    for (int value : registerValues) {
                        System.out.println("Address: " + offset++ + ", Value: " + value);
                    }
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try {
                        m.disconnect();
                    } catch (ModbusIOException e1) {
                        e1.printStackTrace();
                    }
                }
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}
