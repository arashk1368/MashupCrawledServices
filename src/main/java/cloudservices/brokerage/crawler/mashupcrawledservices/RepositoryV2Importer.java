/*
 * Copyright 2014 Arash khodadadi.
 * <http://www.arashkhodadadi.com/>
 */
package cloudservices.brokerage.crawler.mashupcrawledservices;

import cloudservices.brokerage.commons.utils.file_utils.DirectoryUtil;
import cloudservices.brokerage.commons.utils.logging.LoggerSetup;
import cloudservices.brokerage.crawler.crawlingcommons.model.DAO.BaseDAO;
import cloudservices.brokerage.crawler.crawlingcommons.model.DAO.DAOException;
import cloudservices.brokerage.crawler.crawlingcommons.model.DAO.v2.RawCrawledServiceDAO;
import cloudservices.brokerage.crawler.crawlingcommons.model.DAO.v3.ServiceDescriptionDAO;
import cloudservices.brokerage.crawler.crawlingcommons.model.entities.v2.RawCrawledService;
import cloudservices.brokerage.crawler.crawlingcommons.model.entities.v3.ServiceDescription;
import cloudservices.brokerage.crawler.crawlingcommons.model.enums.v3.ServiceDescriptionColType;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.hibernate.cfg.Configuration;

/**
 *
 * @author Arash Khodadadi <http://www.arashkhodadadi.com/>
 */
public class RepositoryV2Importer {

    private static int ServiceUpdatedNum = 0;
    private static int ServiceSavedNum = 0;
    private static int ServiceFoundNum = 0;
    private final static String TOKEN = ";;;";

    private final static Logger LOGGER = Logger.getLogger(RepositoryV2Importer.class.getName());

    public static void main(String[] args) {
        createLogFile();
//        createNewDB();

        copyFromV2();
        LOGGER.log(Level.INFO, "***AFTER COPY FROM V2***");
        LOGGER.log(Level.INFO, "Number of Services Found: {0}", ServiceFoundNum);
        LOGGER.log(Level.INFO, "Number of Services Updated: {0}", ServiceUpdatedNum);
        LOGGER.log(Level.INFO, "Number of Services Saved: {0}", ServiceSavedNum);
//
    }

    private static void createNewDB() {
        try {
            Configuration configuration = new Configuration();
            configuration.configure("v3hibernate.cfg.xml");
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
            DirectoryUtil.createDir("logs/v3");
            LoggerSetup.setup("logs/v3/" + filename + ".txt", "logs/v3/" + filename + ".html", Level.FINER);
            return true;
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage(), ex);
            return false;
        }
    }

    private static void copyFromV2() {
        try {
            Configuration v3Configuration = new Configuration();
            v3Configuration.configure("v3hibernate.cfg.xml");
            Configuration v2Configuration = new Configuration();
            v2Configuration.configure("v2hibernate.cfg.xml");

            LOGGER.log(Level.INFO, "Starting to Migrate from Version 2 Repository...");

            RawCrawledServiceDAO crawledServiceDAO = new RawCrawledServiceDAO();
            RawCrawledServiceDAO.openSession(v2Configuration);

            List services = crawledServiceDAO.getAll("RawCrawledService");

            ServiceDescriptionDAO sdDAO = new ServiceDescriptionDAO();
            ServiceDescriptionDAO.openSession(v3Configuration);

            for (Object serviceObj : services) {
                RawCrawledService rawCrawledService = (RawCrawledService) serviceObj;
                ServiceDescription sd = new ServiceDescription(rawCrawledService);
                ServiceFoundNum++;
                LOGGER.log(Level.INFO, "Saving or Updating Raw Crawled Service with Id = {0}", rawCrawledService.getId());
                LOGGER.log(Level.FINE, "Saving or Updating Service with URL = {0}", sd.getUrl());
                addOrUpdateService(sd, sdDAO);
            }
            LOGGER.log(Level.INFO, "Migration from Version 2 Repository Successful");
        } catch (DAOException ex) {
            LOGGER.log(Level.SEVERE, "ERROR in reading v2 database", ex);
        } finally {
            RawCrawledServiceDAO.closeSession();
        }
    }

    private static void addOrUpdateService(ServiceDescription sd, ServiceDescriptionDAO sdDAO) throws DAOException {
        ServiceDescription inDB = sdDAO.findByUrl(sd.getUrl());

        if (inDB == null) {
            LOGGER.log(Level.FINE, "There is no service in DB, Saving a new one");
            sdDAO.addServiceDescription(sd);
            ServiceSavedNum++;
        } else {
            LOGGER.log(Level.FINE, "Found the same url with ID = {0} in DB, Trying to update", inDB.getId());

            if (inDB.getTitle().compareTo(sd.getTitle()) != 0) {
                LOGGER.log(Level.FINER, "Titles are different;new one: {0} , indb: {1}", new Object[]{sd.getTitle(), inDB.getTitle()});
                String[] titles = sd.getTitle().split(TOKEN);
                for (String title : titles) {
                    if (!inDB.getTitle().contains(title)) {
                        String newString = inDB.getTitle().concat(TOKEN).concat(title);
                        LOGGER.log(Level.FINER, "Adding Title: {0}", title);
                        if (ServiceDescription.checkLength(newString.length(), ServiceDescriptionColType.TITLE)) {
                            inDB.setTitle(newString);
                        } else {
                            LOGGER.log(Level.WARNING, "Title can not be updated because it is too large!");
                        }
                    }
                }
                inDB.setUpdated(true);
            }

            if (inDB.getDescription().compareTo(sd.getDescription()) != 0) {
                LOGGER.log(Level.FINER, "Descriptions are different;new one: {0} , indb: {1}", new Object[]{sd.getDescription(), inDB.getDescription()});
                String[] descriptions = sd.getDescription().split(TOKEN);
                for (String str : descriptions) {
                    if (!inDB.getDescription().contains(str)) {
                        String newString = inDB.getDescription().concat(TOKEN).concat(str);
                        LOGGER.log(Level.FINER, "Adding Description: {0}", str);
                        if (ServiceDescription.checkLength(newString.length(), ServiceDescriptionColType.DESCRIPTION)) {
                            inDB.setDescription(newString);
                        } else {
                            LOGGER.log(Level.WARNING, "Description can not be updated because it is too large!");
                        }
                    }
                }
                inDB.setUpdated(true);
            }

            if (inDB.getSource().compareTo(sd.getSource()) != 0) {
                LOGGER.log(Level.FINER, "Sources are different;new one: {0} , indb: {1}", new Object[]{sd.getSource(), inDB.getSource()});
                String[] newOnes = sd.getSource().split(TOKEN);
                for (String str : newOnes) {
                    if (!inDB.getSource().contains(str)) {
                        String newString = inDB.getSource().concat(TOKEN).concat(str);
                        LOGGER.log(Level.FINER, "Adding Source: {0}", str);
                        if (ServiceDescription.checkLength(newString.length(), ServiceDescriptionColType.SOURCE)) {
                            inDB.setSource(newString);
                        } else {
                            LOGGER.log(Level.WARNING, "Source can not be updated because it is too large!");
                        }
                    }
                }
                inDB.setUpdated(true);
            }

            if (inDB.getTags().compareTo(sd.getTags()) != 0) {
                LOGGER.log(Level.FINER, "Tags are different;new one: {0} , indb: {1}", new Object[]{sd.getTags(), inDB.getTags()});
                String[] newOnes = sd.getTags().split(TOKEN);
                for (String str : newOnes) {
                    if (!inDB.getTags().contains(str)) {
                        String newString = inDB.getTags().concat(TOKEN).concat(str);
                        LOGGER.log(Level.FINER, "Adding Tag: {0}", str);
                        if (ServiceDescription.checkLength(newString.length(), ServiceDescriptionColType.TAGS)) {
                            inDB.setTags(newString);
                        } else {
                            LOGGER.log(Level.WARNING, "Tags can not be updated because it is too large!");
                        }
                    }
                }
                inDB.setUpdated(true);
            }

            if (inDB.getExtraInfo().compareTo(sd.getExtraInfo()) != 0) {
                LOGGER.log(Level.FINER, "Extra info are different;new one: {0} , indb: {1}", new Object[]{sd.getExtraInfo(), inDB.getExtraInfo()});
                String[] newOnes = sd.getExtraInfo().split(TOKEN);
                for (String str : newOnes) {
                    if (!inDB.getExtraInfo().contains(str)) {
                        String newString = inDB.getExtraInfo().concat(TOKEN).concat(str);
                        LOGGER.log(Level.FINER, "Adding Extra Info: {0}", str);
                        if (ServiceDescription.checkLength(newString.length(), ServiceDescriptionColType.EXTRA_INFO)) {
                            inDB.setExtraInfo(newString);
                        } else {
                            LOGGER.log(Level.WARNING, "Extra info can not be updated because it is too large!");
                        }
                    }
                }
                inDB.setUpdated(true);
            }

            if (inDB.isUpdated()) {
                sdDAO.saveOrUpdate(inDB);
                ServiceUpdatedNum++;
            }
        }
    }
}
