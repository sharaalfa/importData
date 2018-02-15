package ru.roskazna.gisgmp.react.impl;


import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ValueSingletonInterface extends Remote {
    //String JNDI_NAME = "JobSingletonService";
    String getVal() throws RemoteException;
    void setVal(String val) throws RemoteException;
}
