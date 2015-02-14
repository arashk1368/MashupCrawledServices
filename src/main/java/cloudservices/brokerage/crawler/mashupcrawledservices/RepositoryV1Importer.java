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
public class RepositoryV1Importer {

    private final static Logger LOGGER = Logger.getLogger(RepositoryV1Importer.class.getName());

    public static void copyWSDLFromV1() {
        try {
            Configuration v1Configuration = new Configuration();
            v1Configuration.configure("v1hibernate.cfg.xml");
            Configuration v2Configuration = new Configuration();
            v2Configuration.configure("v2hibernate.cfg.xml");

            LOGGER.log(Level.INFO, "Starting to Migrate from Version 1 Repository...");

            WSDLDAO wsdlDAO = new WSDLDAO();
            WSDLDAO.openSession(v1Configuration);

            List wsdls = wsdlDAO.getAll("WSDL");

            RawCrawledServiceDAO crawledServiceDAO = new RawCrawledServiceDAO();
            RawCrawledServiceDAO.openSession(v2Configuration);

            for (Object wsdlObj : wsdls) {
                WSDL wsdl = (WSDL) wsdlObj;
                RawCrawledService rcs = ConvertWSDLToCrawledService(wsdl);
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

    private static RawCrawledService ConvertWSDLToCrawledService(WSDL wsdl) {
        LOGGER.log(Level.INFO, "Converting wsdl with Id = {0}", wsdl.getId());
        RawCrawledService rcs = new RawCrawledService();
        rcs.setDescription(wsdl.getDescription());
        rcs.setTitle(wsdl.getTitle());
        rcs.setType(RawCrawledServiceType.WSDL);
        rcs.setUrl(wsdl.getUrl());
        String query = wsdl.getQuery();
        String source = "Google Crawler";
        if (query.compareTo("Service-Repository;;;XMethods") == 0
                || query.compareTo("XMethods;;;Service-Repository") == 0) {
            source = "Service-Repository;;;XMethods";
            query = "";
        } else if (query.compareTo("Service-Repository") == 0) {
            source = "Service-Repository";
            query = "";
        } else if (query.compareTo("XMethods") == 0) {
            source = "XMethods";
            query = "";
        }
        if (query.contains("Service-Repository")) {
            query = query.replace("Service-Repository;;;", ""); // if it is at first or middle
            query = query.replace(";;;Service-Repository", ""); // if it is at the end
            source = source.concat(App.TOKEN).concat("Service-Repository");
        }
        if (query.contains("XMethods")) {
            query = query.replace("XMethods;;;", ""); // if it is at first or middle
            query = query.replace(";;;XMethods", ""); // if it is at the end
            source = source.concat(App.TOKEN).concat("XMethods");
        }
        rcs.setQuery(query);
        rcs.setSource(source);
        rcs.setUpdated(true);
        return rcs;
    }
}
