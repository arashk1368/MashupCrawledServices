/*
 * Copyright 2014 Arash khodadadi.
 * <http://www.arashkhodadadi.com/>
 */
package cloudservices.brokerage.crawler.mashupcrawledservices;

import cloudservices.brokerage.commons.utils.file_utils.DirectoryUtil;
import cloudservices.brokerage.commons.utils.logging.LoggerSetup;
import cloudservices.brokerage.crawler.crawlingcommons.model.DAO.BaseDAO;
import cloudservices.brokerage.crawler.crawlingcommons.model.DAO.DAOException;
import cloudservices.brokerage.crawler.crawlingcommons.model.DAO.v3.ServiceDescriptionDAO;
import cloudservices.brokerage.crawler.crawlingcommons.model.entities.v3.ServiceDescription;
import cloudservices.brokerage.crawler.crawlingcommons.model.enums.v3.ServiceDescriptionColType;
import cloudservices.brokerage.crawler.crawlingcommons.model.enums.v3.ServiceDescriptionType;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.hibernate.cfg.Configuration;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/**
 *
 * @author Arash Khodadadi <http://www.arashkhodadadi.com/>
 */
public class STag2Importer {

    private final static Logger LOGGER = Logger.getLogger(STag2Importer.class.getName());
    private static int ServiceUpdatedNum = 0;
    private static int ServiceSavedNum = 0;
    private static int ServiceFoundNum = 0;
    private final static String TOKEN = ";;;";
    private final static String STAG_TAG_ADDRESS = "DataSets/STag2/tag.txt";
    private final static String STAG_2_ADDRESS = "DataSets/STag2/";

    public static void main(String[] args) {
        createLogFile();
//        createNewDB();

        addSTag2DS(STAG_2_ADDRESS, STAG_TAG_ADDRESS);

        LOGGER.log(Level.INFO, "Number of Services Found: {0}", ServiceFoundNum);
        LOGGER.log(Level.INFO, "Number of Services Updated: {0}", ServiceUpdatedNum);
        LOGGER.log(Level.INFO, "Number of Services Saved: {0}", ServiceSavedNum);

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

    public static void addSTag2DS(String sTag2Address, String tagFileAddress) {
        LOGGER.log(Level.INFO, "Starting to Add STag 2 Repository...");
        try {
            Configuration v3Configuration = new Configuration();
            v3Configuration.configure("v3hibernate.cfg.xml");
            ServiceDescriptionDAO serviceDescDAO = new ServiceDescriptionDAO();
            ServiceDescriptionDAO.openSession(v3Configuration);

            File tagRepo = new File(tagFileAddress);
            BufferedReader br = new BufferedReader(new FileReader(tagRepo));
            int counter = 1;

            while (counter != 186) {
                String name = br.readLine();
                String tags = br.readLine();
                String[] splited = tags.split(":");
                if (splited.length > 1) {
                    tags = splited[1].replace(",", TOKEN);
                } else {
                    tags = "";
                }
                LOGGER.log(Level.INFO, "Service {0}", counter);
                LOGGER.log(Level.FINE, "Name : {0}", name);
                LOGGER.log(Level.FINE, "Tags : {0}", tags);

                File wsdl = new File(sTag2Address + counter + ".xml");
                LOGGER.log(Level.INFO, "Reading File : {0}", wsdl.getName());
                DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
                dbf.setIgnoringComments(false);
                DocumentBuilder db = dbf.newDocumentBuilder();
                Document doc = db.parse(wsdl);
                String comments = STagImporter.getSeekdaComments(doc);
                if (comments.isEmpty()) {
                    comments = STagImporter.getSeekdaComments(doc.getDocumentElement());
                }
                LOGGER.log(Level.FINE, "Comments found: {0}", comments);
                String url = STagImporter.getURLFromWSDLComments(comments);
                if (url.isEmpty()) {
                    LOGGER.log(Level.INFO, "No url found!");
                } else {
                    ServiceFoundNum++;
                    ServiceDescription serviceDesc = new ServiceDescription();
                    serviceDesc.setSource("STag2");
                    serviceDesc.setTitle(name);
                    serviceDesc.setTags(tags);
                    serviceDesc.setType(ServiceDescriptionType.WSDL);
                    serviceDesc.setUpdated(true);
                    serviceDesc.setUrl(url);
                    LOGGER.log(Level.FINE, "Saving or Updating Service with URL = {0}", serviceDesc.getUrl());
                    addOrUpdateService(serviceDesc, serviceDescDAO);
                }

                counter++;
            }
            LOGGER.log(Level.INFO, "Adding STag 2 Repository Successful");
        } catch (DAOException ex) {
            LOGGER.log(Level.SEVERE, "ERROR in reading v3 database", ex);
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "ERROR in reading file", ex);
        } catch (ParserConfigurationException | SAXException ex) {
            LOGGER.log(Level.SEVERE, "ERROR in parsing", ex);
        } finally {
            ServiceDescriptionDAO.closeSession();
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
