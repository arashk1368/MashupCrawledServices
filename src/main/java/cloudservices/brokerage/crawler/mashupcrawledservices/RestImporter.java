/*
 * Copyright 2014 Arash khodadadi.
 * <http://www.arashkhodadadi.com/>
 */
package cloudservices.brokerage.crawler.mashupcrawledservices;

import cloudservices.brokerage.crawler.crawlingcommons.model.DAO.DAOException;
import cloudservices.brokerage.crawler.crawlingcommons.model.DAO.WSDLDAO;
import cloudservices.brokerage.crawler.crawlingcommons.model.DAO.v2.RawCrawledServiceDAO;
import cloudservices.brokerage.crawler.crawlingcommons.model.entities.WSDL;
import cloudservices.brokerage.crawler.crawlingcommons.model.entities.v2.RawCrawledService;
import cloudservices.brokerage.crawler.crawlingcommons.model.enums.v2.RawCrawledServiceType;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.hibernate.cfg.Configuration;

/**
 *
 * @author Arash Khodadadi <http://www.arashkhodadadi.com/>
 */
public class RestImporter {

    private final static Logger LOGGER = Logger.getLogger(RepositoryV1Importer.class.getName());

    public static void copyRESTFromTest() {
        try {
            Configuration testConfiguration = new Configuration();
            testConfiguration.configure("v2testhibernate.cfg.xml");
            Configuration v2Configuration = new Configuration();
            v2Configuration.configure("v2hibernate.cfg.xml");

            LOGGER.log(Level.INFO, "Starting to Migrate from Test Repository...");

            RawCrawledServiceDAO testCrawledServiceDAO = new RawCrawledServiceDAO();
            RawCrawledServiceDAO.openSession(testConfiguration);

            List rests = testCrawledServiceDAO.getAll("RawCrawledService");

            RawCrawledServiceDAO crawledServiceDAO = new RawCrawledServiceDAO();
            RawCrawledServiceDAO.openSession(v2Configuration);

            for (Object restObj : rests) {
                RawCrawledService rest = (RawCrawledService) restObj;
                RawCrawledService rcs = new RawCrawledService(rest);
                App.ServiceFoundNum++;
                LOGGER.log(Level.FINE, "Saving or Updating Raw Crawled Service with URL = {0}", rcs.getUrl());
                App.addOrUpdateCrawledService(rcs, crawledServiceDAO);
            }
            LOGGER.log(Level.INFO, "Migration from Version 1 Repository Successful");
        } catch (DAOException ex) {
            LOGGER.log(Level.SEVERE, "ERROR in reading v1 database", ex);
        } finally {
            RawCrawledServiceDAO.closeSession();
        }
    }
}
