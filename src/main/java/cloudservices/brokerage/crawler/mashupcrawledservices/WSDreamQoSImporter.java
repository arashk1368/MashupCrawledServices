/*
 * Copyright 2014 Arash khodadadi.
 * <http://www.arashkhodadadi.com/>
 */
package cloudservices.brokerage.crawler.mashupcrawledservices;

import cloudservices.brokerage.crawler.crawlingcommons.model.DAO.BaseDAO;
import cloudservices.brokerage.crawler.crawlingcommons.model.DAO.DAOException;
import cloudservices.brokerage.crawler.crawlingcommons.model.DAO.v2.RawCrawledServiceDAO;
import cloudservices.brokerage.crawler.crawlingcommons.model.entities.v2.RawCrawledService;
import cloudservices.brokerage.crawler.crawlingcommons.model.enums.v2.RawCrawledServiceType;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.hibernate.cfg.Configuration;

/**
 *
 * @author Arash Khodadadi <http://www.arashkhodadadi.com/>
 */
public class WSDreamQoSImporter {

    private final static Logger LOGGER = Logger.getLogger(WSDreamQoSImporter.class.getName());

    public static void addWSDreamDS(String dsAddress) {
        LOGGER.log(Level.INFO, "Starting to Add WS Dream QoS Repository...");

        File repository = new File(dsAddress);
        BufferedReader br = null;
        String line;
        String splitBy = "" + (char) 9;

        try {

            br = new BufferedReader(new FileReader(repository));
            RawCrawledService rcs;

            Configuration v2Configuration = new Configuration();
            v2Configuration.configure("v2hibernate.cfg.xml");
            RawCrawledServiceDAO crawledServiceDAO = new RawCrawledServiceDAO();
            RawCrawledServiceDAO.openSession(v2Configuration);

            while ((line = br.readLine()) != null) {
                String[] row = line.split(splitBy);
                // at least url is neccessary
                if (row.length > 1) {
                    rcs = new RawCrawledService();
                    App.ServiceFoundNum++;
                    rcs.setSource("WSDreamQoS");
                    rcs.setUrl(row[1]);
                    rcs.setUpdated(true);
                    rcs.setType(RawCrawledServiceType.WSDL);
                    LOGGER.log(Level.FINE, "Saving or Updating Raw Crawled Service with URL = {0}", rcs.getUrl());
                    App.addOrUpdateCrawledService(rcs, crawledServiceDAO);
                }
            }
            LOGGER.log(Level.INFO, "Addition from WS Dream QoS Repository Successful");
        } catch (FileNotFoundException e) {
            LOGGER.log(Level.SEVERE, "WS Dream QoS File Not Found", e);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "WS Dream QoS File IO Exception", e);
        } catch (DAOException ex) {
            LOGGER.log(Level.SEVERE, "ERROR in Accessing Database", ex);
        } finally {
            BaseDAO.closeSession();
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE, "WS Dream QoS File IO Exception", e);
                }
            }
        }
    }
}
