using LiquidusData;
using Microsoft.VisualBasic.FileIO;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace DDDImport
{
    class Program
    {
        private static string ConnectionString;

        public enum DealerListingUsageAll
        {
            DealerID,
            StockNumber,
            InventoryDate,
            Type, //Type_NewOrUsed,
            Status,
            InvoicePrice,
            PackAmount,
            Cost,
            price, //ListPrice,
            MSRP,
            LotLocation,
            Condition,
            Tagline,
            IsCpo, //IsCertified,
            CertificationNumber,
            vin, //VIN,
            make, //Make,
            model, //Model,
            year, //ModelYear,
            ModelCode,
            Trim, //TrimLevel,
            SubTrimLevel,
            Classification,
            VehicleTypeCode,
            miles, //Odometer,
            PayloadCapacity,
            SeatingCapacity,
            WheelBase,
            bodyStyle, //BodyDescription,
            BodyDoorCount,
            DriveTrainDescription,
            EngineDesc, //EngineDescription,
            EngineCylinderCount,
            TransDesc, //TransmissionDescription,
            TransmissionType,
            ExtColor, //ExteriorColorDescription,
            ExteriorColorBase,
            InteriorDescription,
            InteriorColor,
            StandardFeatures, //pipe delimited multi-value
            DealerFeatures, //DealerAddedFeatures, //pipe delimited multi-value
            TechnicalFeatures,
            SafetyFeatures,
            InteriorFeatures,
            ExteriorFeatures,
            ElectronicFeatures,
            OtherFeatures,
            CreatedDate,
            LastModifiedDate,
            ModifiedFlag,
            DealerName,
            DealerAddress,
            DealerCity,
            DealerState,
            DealerPostalCode,
            DealerPhoneNumber,
            mid, //MediaID,
            thumbnail, //ImageURLs,
            URL, //VehicleURl
        }

        //if you want to update more file fields to the database, just put the field name in the enum below and above in the order the field appears in the file
        public enum DealerListingUsageForDDD
        {
            DealerID,
            StockNumber,
            Type,
            price,
            IsCpo,
            vin,
            make,
            model,
            year,
            Trim,
            miles,
            bodyStyle,
            EngineDesc,
            TransDesc,
            ExtColor,
            DealerFeatures,  
            TechnicalFeatures,
            SafetyFeatures,
            InteriorFeatures,
            ExteriorFeatures,
            ElectronicFeatures,
            OtherFeatures,
            mid,
            thumbnail,
            URL
        }

        static void Main(string[] args)
        {
            ConnectionString = ConfigurationManager.ConnectionStrings["ApplicationConnectionString"].ConnectionString.ToString();

            Data ldCampaigns = new Data(ConnectionString);
            DataTable dt = ldCampaigns.GetDataTable("CampaignGet");

            foreach (DataRow dr in dt.Rows)
            {
                int CampaignID = (int)dr["CampaignID"];
                DoCampaign(CampaignID);
            }
        }

        private static void DoCampaign(int campaignID)
        {
            Data ldDealer = new Data(ConnectionString);
            ldDealer.AddParameter("CampaignID", campaignID);
            DataTable dt = ldDealer.GetDataTable("CampaignDealerGet");
            foreach (DataRow dr in dt.Rows)
            {
                int DealerID = (int)dr["DealerID"];
                int SourceID = (int)dr["SourceID"];
                string SourceDealerID = dr["SourceDealerID"].ToString();
                DoDealer(DealerID, campaignID, SourceDealerID);
            }
        }

        private static void DoDealer(int dealerID, int CampaignID, string SourceDealerID)
        {
            // Clear old
            Data ldClear = new Data(ConnectionString);
            ldClear.AddParameter("CampaignID", CampaignID);
            ldClear.AddParameter("DealerID", dealerID);
            ldClear.AddParameter("Delete", 1);
            ldClear.ExecuteNonQuery("CampaignDealerListingSet");

            string DealerPath = string.Format(ConfigurationManager.AppSettings.Get("DealerPath")+"{0}.txt", SourceDealerID); 
            if (File.Exists(DealerPath))
            {
                using (TextFieldParser DMiReader = new TextFieldParser(DealerPath))
                {
                    DMiReader.TextFieldType = FieldType.Delimited;
                    DMiReader.Delimiters = new string[] { "\t" }; //just set the column delimeter (tab)
                    DMiReader.HasFieldsEnclosedInQuotes = false;

                    //read each row (since parser is automatically newline delimeted)
                    while (!DMiReader.EndOfData)
                    {
                        List<KeyValuePair<string, object>> sqlParams = new List<KeyValuePair<string, object>>();

                        //parse row for columns
                        string[] Listing = DMiReader.ReadFields();

                        //for each column
                        for (int clmIdx = 0; clmIdx < Listing.Length; clmIdx++)
                        {
                            if (clmIdx == 0)
                            {
                                KeyValuePair<string, object> paramAdditional = new KeyValuePair<string, object>("DealerID", dealerID);
                                sqlParams.Add(paramAdditional);
                            }
                            else
                            {
                                //generate parameters for sproc
                                KeyValuePair<string, object> param = new KeyValuePair<string, object>(((DealerListingUsageAll)clmIdx).ToString(), Listing[clmIdx]);
                                if (Enum.GetNames(typeof(DealerListingUsageForDDD)).Contains(param.Key))
                                    if (param.Key != "thumbnail" && param.Key != "price")
                                        sqlParams.Add(param);

                                //add this param manually because the file value needs to be duplicated for another param for "vid"
                                if (param.Key == "StockNumber")
                                {
                                    KeyValuePair<string, object> paramAdditional = new KeyValuePair<string, object>("vid", Listing[clmIdx]);
                                    sqlParams.Add(paramAdditional);
                                }

                                //also do this manually because the file value needs to be duplicated for another param for "ImageList" and the original ("thumbnail") needs to be modified
                                if (param.Key == "thumbnail")
                                {
                                    KeyValuePair<string, object> paramModified = new KeyValuePair<string, object>("thumbnail", Listing[clmIdx].Split('|').FirstOrDefault());
                                    sqlParams.Add(paramModified);
                                    KeyValuePair<string, object> paramAdditional = new KeyValuePair<string, object>("ImageList", Listing[clmIdx]);
                                    sqlParams.Add(paramAdditional);
                                }

                                //format price from ##.00 to without decimals as sproc param is int
                                if (param.Key == "price")
                                {
                                    string priceInFile = Listing[clmIdx];

                                    double priceForDB = 0;

                                    if (!string.IsNullOrEmpty(priceInFile))
                                       priceForDB =  double.Parse(priceInFile.Split(',')[0]);

                                    KeyValuePair<string, object> paramAdditional = new KeyValuePair<string, object>("price", priceForDB);
                                    sqlParams.Add(paramAdditional);
                                }
                            }
                        }

                        Data ldListingSet = new Data(ConnectionString);
                        ldListingSet.AddParameter("CampaignID", CampaignID);

                        //add the rest of the params
                        foreach (KeyValuePair<string, object> par in sqlParams)
                            ldListingSet.AddParameter(par.Key, par.Value);

                        //call the sproc finally
                        ldListingSet.ExecuteNonQuery("CampaignDealerListingSet");

                        ldListingSet.ClearParameter();
                    }
                }
            }
        }
    }
}

