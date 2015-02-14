package cloudservices.brokerage.crawler.mashupcrawledservices;

import cloudservices.brokerage.commons.utils.file_utils.DirectoryUtil;
import cloudservices.brokerage.commons.utils.logging.LoggerSetup;
import cloudservices.brokerage.crawler.crawlingcommons.model.DAO.BaseDAO;
import cloudservices.brokerage.crawler.crawlingcommons.model.DAO.DAOException;
import cloudservices.brokerage.crawler.crawlingcommons.model.DAO.v2.RawCrawledServiceDAO;
import cloudservices.brokerage.crawler.crawlingcommons.model.entities.v2.RawCrawledService;
import cloudservices.brokerage.crawler.crawlingcommons.model.enums.v2.RawCrawledServiceColType;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.hibernate.cfg.Configuration;

public class App {

    public static int ServiceUpdatedNum = 0;
    public static int ServiceSavedNum = 0;
    public static int ServiceFoundNum = 0;
    public final static String TOKEN = ";;;";

    private final static Logger LOGGER = Logger.getLogger(App.class.getName());
    private final static String STAG_DS_CSV_ADDRESS = "DataSets/STag1.0/Service.csv";
    private final static String STAG_DS_ADDRESS = "DataSets/STag1.0/WSDLs";
    private final static String WSDREAM_QOS_DS_ADDRESS = "DataSets/WSDream-QoSDataset2/wslist.txt";

    public static void main(String[] args) {
        createLogFile();
        createNewDB();

//        RepositoryV1Importer.copyWSDLFromV1();
//
//        LOGGER.log(Level.INFO, "***AFTER COPY WSDLS FROM V1***");
//        LOGGER.log(Level.INFO, "Number of Services Found: {0}", ServiceFoundNum);
//        LOGGER.log(Level.INFO, "Number of Services Updated: {0}", ServiceUpdatedNum);
//        LOGGER.log(Level.INFO, "Number of Services Saved: {0}", ServiceSavedNum);

//        RepositoryV1Importer.copyWADLFromV1();
//
//        LOGGER.log(Level.INFO, "***AFTER COPY WADLS FROM V1***");
//        LOGGER.log(Level.INFO, "Number of Services Found: {0}", ServiceFoundNum);
//        LOGGER.log(Level.INFO, "Number of Services Updated: {0}", ServiceUpdatedNum);
//        LOGGER.log(Level.INFO, "Number of Services Saved: {0}", ServiceSavedNum);

//        ServiceFoundNum = 0;
//        ServiceUpdatedNum = 0;
//        ServiceSavedNum = 0;
//
//        WSDreamQoSImporter.addWSDreamDS(WSDREAM_QOS_DS_ADDRESS);
//
//        LOGGER.log(Level.INFO, "***AFTER ADDING WS Dream QoS Repository***");
//        LOGGER.log(Level.INFO, "Number of Services Found: {0}", ServiceFoundNum);
//        LOGGER.log(Level.INFO, "Number of Services Updated: {0}", ServiceUpdatedNum);
//        LOGGER.log(Level.INFO, "Number of Services Saved: {0}", ServiceSavedNum);
        ////      This has  ... problems!
//        ServiceFoundNum = 0;
//        ServiceUpdatedNum = 0;
//        ServiceSavedNum = 0;
//        STagImporter.addSTagDSFromCSV(STAG_DS_CSV_ADDRESS);
//
//        LOGGER.log(Level.INFO, "***AFTER ADDING STag Repository***");
//        LOGGER.log(Level.INFO, "Number of Services Found: {0}", ServiceFoundNum);
//        LOGGER.log(Level.INFO, "Number of Services Updated: {0}", ServiceUpdatedNum);
//        LOGGER.log(Level.INFO, "Number of Services Saved: {0}", ServiceSavedNum);
//        ServiceFoundNum = 0;
//        ServiceUpdatedNum = 0;
//        ServiceSavedNum = 0;
//
//        STagImporter.addSTagDS(STAG_DS_CSV_ADDRESS, STAG_DS_ADDRESS);
//
//        LOGGER.log(Level.INFO, "***AFTER ADDING STag Repository***");
//        LOGGER.log(Level.INFO, "Number of Services Found: {0}", ServiceFoundNum);
//        LOGGER.log(Level.INFO, "Number of Services Updated: {0}", ServiceUpdatedNum);
//        LOGGER.log(Level.INFO, "Number of Services Saved: {0}", ServiceSavedNum);
    }

    public static void addOrUpdateCrawledService(RawCrawledService rcs, RawCrawledServiceDAO crawledServiceDAO) throws DAOException {
        RawCrawledService inDB = crawledServiceDAO.findByUrl(rcs.getUrl());

        if (inDB == null) {
            LOGGER.log(Level.FINE, "There is no raw crawled service in DB, Saving a new one");
            crawledServiceDAO.addCrawledService(rcs);
            ServiceSavedNum++;
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
                inDB.setUpdated(true);
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
                inDB.setUpdated(true);
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
                inDB.setUpdated(true);
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
                inDB.setUpdated(true);
            }

            if (inDB.getExtraContext().compareTo(rcs.getExtraContext()) != 0) {
                LOGGER.log(Level.FINER, "Extra context are different;new one: {0} , indb: {1}", new Object[]{rcs.getExtraContext(), inDB.getExtraContext()});
                String[] newOnes = rcs.getExtraContext().split(TOKEN);
                for (String str : newOnes) {
                    if (!inDB.getExtraContext().contains(str)) {
                        String newString = inDB.getExtraContext().concat(TOKEN).concat(str);
                        LOGGER.log(Level.FINER, "Adding Extra Context: {0}", str);
                        if (RawCrawledService.checkLength(newString.length(), RawCrawledServiceColType.EXTRA_CONTEXT)) {
                            inDB.setExtraContext(newString);
                        } else {
                            LOGGER.log(Level.WARNING, "Extra context can not be updated because it is too large!");
                        }
                    }
                }
                inDB.setUpdated(true);
            }

            if (inDB.isUpdated()) {
                crawledServiceDAO.saveOrUpdate(inDB);
                ServiceUpdatedNum++;
            }
        }
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

}
