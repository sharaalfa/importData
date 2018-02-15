package ru.roskazna.gisgmp.react;





import ru.roskazna.gisgmp.react.impl.ValueSingletonInterface;
import weblogic.cluster.singleton.SingletonService;


import javax.interceptor.AroundInvoke;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.io.Serializable;


public class JobSingletonService implements SingletonService, Serializable, ValueSingletonInterface {

    private static final long serialVersionUID = 3966807367110330202L;
    private static final String jndiName = "JobSingletonService";
    private String val;

    public JobSingletonService() {
        super();
    }

    @Override
    public String getVal() {
        return val;
    }

    @Override
    public synchronized void setVal(String val) {
        this.val = val;
    }

    //@AroundInvoke
    public void activate() {
        System.out.println("activated");
        Context ic = null;
        try {
            ic = new InitialContext();
            this.val = System.getProperty("weblogic.Name");
            ic.bind(jndiName, this);
            System.out.println("Кластер сейчас завязан в JNDI с" + jndiName);


        } catch (NamingException e) {
            this.val = "game over!";
            e.printStackTrace();
        } finally {
            try {
                if(ic != null) ic.close();
            } catch (NamingException e) {
                e.printStackTrace();
            }
        }



    }

    public void deactivate(){
        System.out.println("deactivated");
        Context ic = null;
        try {
            //Environment environment = new Environment();
           // Context ctx = environment.getInitialContext();
            //ctx.unbind(JNDI_NAME);
            ic = new InitialContext();
            ic.unbind(jndiName);
            System.out.println("Контекст о работе с кассандрой успешно деактивирован");
        } catch (NamingException e) {
            e.printStackTrace();
        }

    }
}
