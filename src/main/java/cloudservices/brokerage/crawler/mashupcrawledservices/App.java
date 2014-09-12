package cloudservices.brokerage.crawler.mashupcrawledservices;

import cloudservices.brokerage.commons.utils.file_utils.DirectoryUtil;
import cloudservices.brokerage.commons.utils.logging.LoggerSetup;
import cloudservices.brokerage.crawler.crawlingcommons.model.DAO.BaseDAO;
import cloudservices.brokerage.crawler.crawlingcommons.model.DAO.DAOException;
import cloudservices.brokerage.crawler.crawlingcommons.model.DAO.WSDLDAO;
import cloudservices.brokerage.crawler.crawlingcommons.model.DAO.v2.RawCrawledServiceDAO;
import cloudservices.brokerage.crawler.crawlingcommons.model.entities.WSDL;
import cloudservices.brokerage.crawler.crawlingcommons.model.entities.v2.RawCrawledService;
import cloudservices.brokerage.crawler.crawlingcommons.model.enums.v2.RawCrawledServiceColType;
import cloudservices.brokerage.crawler.crawlingcommons.model.enums.v2.RawCrawledServiceType;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.hibernate.cfg.Configuration;

public class App {

    private final static Logger LOGGER = Logger.getLogger(App.class.getName());
    private final static String TOKEN = ";;;";

    public static void main(String[] args) {
        createLogFile();
//        createNewDB();
        copyWSDLFromV1();
    }

    private static void createNewDB() {
        try {
            Configuration configuration = new Configuration();
            configuration.configure("v2hibernate.cfg.xml");
            BaseDAO.openSession(configuration);
            LOGGER.log(Level.INFO, "Database Creation Successful");
        } finally {
            BaseDAO.closeSession();
        }
    }

    private static boolean createLogFile() {
        try {
            StringBuilder sb = new StringBuilder();
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm");
            Calendar cal = Calendar.getInstance();
            sb.append(dateFormat.format(cal.getTime()));
            String filename = sb.toString();
            DirectoryUtil.createDir("logs");
            LoggerSetup.setup("logs/" + filename + ".txt", "logs/" + filename + ".html", Level.FINER);
            return true;
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage(), ex);
            return false;
        }
    }

    private static void copyWSDLFromV1() {
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
                LOGGER.log(Level.FINE, "Saving or Updating Raw Crawled Service with URL = {0}", rcs.getUrl());
                addOrUpdateCrawledService(rcs, crawledServiceDAO);
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
            source = source.concat(TOKEN).concat("Service-Repository");
        }
        if (query.contains("XMethods")) {
            query = query.replace("XMethods;;;", ""); // if it is at first or middle
            query = query.replace(";;;XMethods", ""); // if it is at the end
            source = source.concat(TOKEN).concat("XMethods");
        }
        rcs.setQuery(query);
        rcs.setSource(source);
        rcs.setIsUpdated(true);
        return rcs;
    }

    private static void addOrUpdateCrawledService(RawCrawledService rcs, RawCrawledServiceDAO crawledServiceDAO) throws DAOException {
        RawCrawledService inDB = crawledServiceDAO.findByUrl(rcs.getUrl());

        if (inDB == null) {
            LOGGER.log(Level.FINE, "There is no raw crawled service in DB, Saving a new one");
            crawledServiceDAO.addCrawledService(rcs);
        } else {
            LOGGER.log(Level.FINE, "Found the same url with ID = {0} in DB, Trying to update", inDB.getId());

            if (inDB.getTitle().compareTo(rcs.getTitle()) != 0) {
                LOGGER.log(Level.FINER, "Titles are different;new one: {0} , indb: {1}", new Object[]{rcs.getTitle(), inDB.getTitle()});
                String[] titles = rcs.getTitle().split(TOKEN);
                for (String title : titles) {
                    if (!inDB.getTitle().contains(title)) {
                        String newString = inDB.getTitle().concat(TOKEN).concat(title);
                        LOGGER.log(Level.FINER, "Adding Title: {0}", title);
                        if (RawCrawledService.checkLength(newString.length(), RawCrawledServiceColType.TITLE)) {
                            inDB.setTitle(newString);
                        } else {
                            LOGGER.log(Level.WARNING, "Title can not be updated because it is too large!");
                        }
                    }
                }
                inDB.setIsUpdated(true);
            }

            if (inDB.getDescription().compareTo(rcs.getDescription()) != 0) {
                LOGGER.log(Level.FINER, "Descriptions are different;new one: {0} , indb: {1}", new Object[]{rcs.getDescription(), inDB.getDescription()});
                String[] descriptions = rcs.getDescription().split(TOKEN);
                for (String str : descriptions) {
                    if (!inDB.getDescription().contains(str)) {
                        String newString = inDB.getDescription().concat(TOKEN).concat(str);
                        LOGGER.log(Level.FINER, "Adding Description: {0}", str);
                        if (RawCrawledService.checkLength(newString.length(), RawCrawledServiceColType.DESCRIPTION)) {
                            inDB.setDescription(newString);
                        } else {
                            LOGGER.log(Level.WARNING, "Description can not be updated because it is too large!");
                        }
                    }
                }
                inDB.setIsUpdated(true);
            }

            if (inDB.getSource().compareTo(rcs.getSource()) != 0) {
                LOGGER.log(Level.FINER, "Sources are different;new one: {0} , indb: {1}", new Object[]{rcs.getSource(), inDB.getSource()});
                String[] newOnes = rcs.getSource().split(TOKEN);
                for (String str : newOnes) {
                    if (!inDB.getSource().contains(str)) {
                        String newString = inDB.getSource().concat(TOKEN).concat(str);
                        LOGGER.log(Level.FINER, "Adding Source: {0}", str);
                        if (RawCrawledService.checkLength(newString.length(), RawCrawledServiceColType.SOURCE)) {
                            inDB.setSource(newString);
                        } else {
                            LOGGER.log(Level.WARNING, "Source can not be updated because it is too large!");
                        }
                    }
                }
                inDB.setIsUpdated(true);
            }

            if (inDB.getQuery().compareTo(rcs.getQuery()) != 0) {
                LOGGER.log(Level.FINER, "Queries are different;new one: {0} , indb: {1}", new Object[]{rcs.getQuery(), inDB.getQuery()});
                String[] newOnes = rcs.getQuery().split(TOKEN);
                for (String str : newOnes) {
                    if (!inDB.getQuery().contains(str)) {
                        String newString = inDB.getQuery().concat(TOKEN).concat(str);
                        LOGGER.log(Level.FINER, "Adding Query: {0}", str);
                        if (RawCrawledService.checkLength(newString.length(), RawCrawledServiceColType.SEARCHED_QUERY)) {
                            inDB.setQuery(newString);
                        } else {
                            LOGGER.log(Level.WARNING, "Query can not be updated because it is too large!");
                        }
                    }
                }
                inDB.setIsUpdated(true);
            }

            crawledServiceDAO.saveOrUpdate(inDB);
        }
    }
}
