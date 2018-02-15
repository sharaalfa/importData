package ru.roskazna.gisgmp.react;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.roskazna.gisgmp.react.impl.JobSingeltonServiceBeanLocal;
import ru.roskazna.gisgmp.react.impl.JobSingletonServiceBeanRemote;
import ru.roskazna.gisgmp.react.impl.ValueSingletonInterface;
import javax.annotation.PostConstruct;
import javax.ejb.Stateless;
import javax.interceptor.Interceptors;
import javax.jws.WebMethod;
import javax.jws.WebService;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.util.Calendar;
import java.util.Date;
import java.util.Timer;

@WebService(name="JobSingleton", serviceName = "JobSingletonServiceBean")
@Stateless(mappedName = "ru.roskazna.gisgmp.react.JobSingletonServiceBean")
public class JobSingeltonServiceBean implements JobSingletonServiceBeanRemote, JobSingeltonServiceBeanLocal {
    private static final Logger logger = LoggerFactory.getLogger(JobSingeltonServiceBean.class);

    //@Interceptors(JobSingletonService.class)
    //@PostConstruct
    @WebMethod
    public void getJobClient() throws Exception {
        logger.info("sdghfhdosgasdoilhlosdf");
        Context ctx = new InitialContext();
        ValueSingletonInterface valueSingletonInterface = (ValueSingletonInterface) ctx.lookup("JobSingletonService");
        logger.info("" + valueSingletonInterface);
        logger.info("" + valueSingletonInterface.getVal());



    }


}
