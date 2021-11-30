
# Empty environment.
rm(list = ls())

## Enter/Assign data folder path.
fpath <- "..."

## Sample data to test the code (to run each, uncomment the folder path listed below):
# -COMLEX/USMLE (Qbank)
fpath <- "G:\\Shared drives\\Psychometrics\\KNA LAP\\_Programs\\USMLE\\Item Analysis\\2021 COMLEX QBank\\Data"
# fpath <- "G:\\Shared drives\\Psychometrics\\KNA LAP\\_Programs\\USMLE\\Item Analysis\\2021 USMLE QBank\\Data"
# -MCAT
# fpath <- "G:\\Shared drives\\Psychometrics\\KNA LAP\\_Programs\\MCAT\\Item Analysis\\2021_foundational_course\\data\\BioChem"
# fpath <- "G:\\Shared drives\\Psychometrics\\KNA LAP\\_Programs\\MCAT\\Item Analysis\\2021_foundational_course\\data\\BehSci"
# -BAR
# fpath <-"G:\\Shared drives\\Psychometrics\\KNA LAP\\_Programs\\Bar\\Item Analysis\\2021 GBR\\Data\\44"
# fpath <-"G:\\Shared drives\\Psychometrics\\KNA LAP\\_Programs\\Bar\\Item Analysis\\2021 GBR\\Data\\46"
# fpath <- "G:\\Shared drives\\Psychometrics\\KNA LAP\\_Programs\\Bar\\Item Analysis\\2021 Diag Pilot Test\\Data\\cleaned_data" <<<<<<<<<<<


# TO BE RESOLVED: <<<<<<<<<<<
## -input data files: how do we plan on communicating cleaning code and analysis code?
  ### current IA code below reads in all data files from the cleaned data folder.
## -column name for student ID differs across files.
## -differing file structures:
  ### column names can differ across different products: e.g., sequenceName vs. template_name
  ### los_count: where do we get this information?
  ### parent_ID: may not be found in all data files (e.g., Qbank).
  ### skill_level, Topic_1, Topic_2: may not be found in all data files.
## -What to do with items showing in multiple templates: combine responses ???
## -to where and in which format to export the output?



# ITEM ANALYSIS AUTOMATION.
# Load packages.
library(dplyr)
library(stringr)
library(magrittr)
library(tidyr)
library(readr)

# Read in and prep cleaned item response data.
## read file names in the folder.
master <- list.files(fpath)
## remove if there are other types in the folder.
master <- master[str_detect(master, ".csv")]
## load data files into a list.
dflist <- list()
for (i in 1:length(master)) {
  tt <- read_csv(paste0(fpath,"\\",master[i]),
                 col_names = T, 
                 na = c( "-99", NA, "NA", ".")) %>% as.data.frame()
  dflist[[master[i]]] <- tt
}
## detect file names and rename in the data frame list.
names(dflist)[which(str_detect(names(dflist), "User_level_Item_Scores"))] <- "scored"
names(dflist)[which(str_detect(names(dflist), "User_level_Responses"))] <- "response_data"
names(dflist)[which(str_detect(names(dflist), "Content_Item_Info"))] <- "content_info"
names(dflist)[which(str_detect(names(dflist), "Sequence_Level_Info|activity_Level_Info"))] <- "seq_info"
names(dflist)[which(str_detect(names(dflist), "User_Level_Info"))] <- "user"
## match column name for student ID across files.
names(dflist$response_data)[1] <- "ID"
names(dflist$scored)[1] <- "ID"
names(dflist$seq_info)[1] <- "ID"
names(dflist$user)[1] <- "ID"
## match other column names for different databases/product lines.
names(dflist$seq_info)[names(dflist$seq_info) %in% c("sequenceId","template_id","templateId")] <- "templateId"
names(dflist$seq_info)[names(dflist$seq_info) %in% c("sequenceName","template_name","activityName")] <- "activityName"
names(dflist$content_info)[names(dflist$content_info) %in% c("sequenceName","template_name","activityName")] <- "activityName"



# CTT analysis.
out_all <- data.frame()
a <- 0
# for each item:
for (i in 2:ncol(dflist$response_data)) {
  out_i <- data.frame()
  # save item name
  name_i <- names(dflist$response_data)[i]
  # for each response category given item i:
  for (k in 1:dflist$content_info$countchoices[which(dflist$content_info$contentItemName==name_i)]) {
    out_ik <- data.frame("Test_name"=dflist$content_info$activityName[which(dflist$content_info$contentItemName==name_i)],
                         "Test_ID"=dflist$content_info$templateId[which(dflist$content_info$contentItemName==name_i)],
                         "Item_name"=name_i,
                         "QID"=dflist$content_info$contentItemId[which(dflist$content_info$contentItemName==name_i)],
                         # "los_count"=, <<<<<<<<<<<
                         # "parent_ID"=dflist$content_info$parentid[which(dflist$content_info$contentItemName==name_i)],, <<<<<<<<<<<
                         "Item_key"=dflist$content_info$correctAnswer[which(dflist$content_info$contentItemName==name_i)],
                         "n_option"=dflist$content_info$countchoices[which(dflist$content_info$contentItemName==name_i)],
                         # "skill_level"=, <<<<<<<<<<<
                         # "Topic_1"=, <<<<<<<<<<<
                         # "Topic_2"=, <<<<<<<<<<<
                         "n_i"=dflist$content_info$count_att[which(dflist$content_info$contentItemName==name_i)],
                         "n_omit"=dflist$content_info$count_seen[which(dflist$content_info$contentItemName==name_i)] - dflist$content_info$count_att[which(dflist$content_info$contentItemName==name_i)],
                         "n_correct"=dflist$content_info$num_correct[which(dflist$content_info$contentItemName==name_i)],
                         "pvalue"=ifelse(dflist$content_info$count_att[which(dflist$content_info$contentItemName==name_i)] <= 1, NA,
                                         dflist$content_info$num_correct[which(dflist$content_info$contentItemName==name_i)]/
                                           dflist$content_info$count_att[which(dflist$content_info$contentItemName==name_i)]),
                         "pbs_i"=ifelse(dflist$content_info$count_att[which(dflist$content_info$contentItemName==name_i)] <= 1, NA,
                                        dflist$scored %>% 
                                          left_join(dflist$user %>% select(ID, panel_pplus), by = c("ID" = "ID")) %$%
                                          cor(panel_pplus, .[,i], use = "complete.obs")),
                         "pbs_c_i"=ifelse(dflist$content_info$count_att[which(dflist$content_info$contentItemName==name_i)] <= 1, NA,
                                          dflist$scored %>% 
                                            left_join(dflist$user %>% select(ID, total_correct, total_att, panel_pplus), by = c("ID" = "ID")) %>% 
                                            mutate(pplus2 = (total_correct - .[,i])/(total_att-1)) %$%
                                            cor(pplus2, .[,i], use = "complete.obs")),
                         "p_omit"=ifelse(dflist$content_info$count_att[which(dflist$content_info$contentItemName==name_i)] <= 1, NA,
                                         (dflist$content_info$count_seen[which(dflist$content_info$contentItemName==name_i)] - dflist$content_info$count_att[which(dflist$content_info$contentItemName==name_i)]) /
                                           dflist$content_info$count_seen[which(dflist$content_info$contentItemName==name_i)]),
                         "response"=k,
                         "n"=NROW(dflist$response_data[,i][which(dflist$response_data[,i]==k)]),
                         "p"=ifelse(dflist$content_info$count_att[which(dflist$content_info$contentItemName==name_i)] <= 1, NA,
                                    NROW(dflist$response_data[,i][which(dflist$response_data[,i]==k)])/
                                      NROW(dflist$response_data[,i][which(is.na(dflist$response_data[,i])==F)])),
                         "pbs"=ifelse(dflist$content_info$count_att[which(dflist$content_info$contentItemName==name_i)] <= 1, NA,
                                      dflist$response_data %>% 
                                        left_join(dflist$user %>% select(ID, panel_pplus), by = c("ID" = "ID")) %>% 
                                        mutate(binary = ifelse(dflist$response_data[,i]==k,1,0)) %$%
                                        cor(panel_pplus, binary, use = "complete.obs"))
    )
    out_i <- rbind(out_i, out_ik)
  } #end of item loop
  # print item name completed
  a <- a + 1
  cat("[", a, "] Item completed: ", name_i, "\n\n", sep = "")
  # append item stats to the rest
  out_all <- rbind(out_all, out_i)
}


# Flags
final_out <- out_all %>% 
  mutate(across(c(pvalue,pbs_i,pbs_c_i,p_omit,p,pbs), round, 3)) %>%
  group_by(Item_name) %>% 
  mutate(Low_n_flag = ifelse(n_i < 50, "n < 50", NA),
         pvalue_flag = ifelse(is.na(pvalue)==T, NA, 
                              ifelse(pvalue > 0.900, "Too Easy",
                                     ifelse(pvalue < 0.250, "Too Hard", "Acceptable"))),
         pbs_flag = ifelse(is.na(pbs_i)==T, NA, 
                           ifelse(pbs_i >= 0.300, "Excellent",
                                  ifelse(pbs_i < 0.000, "Unacceptable",
                                         ifelse(pbs_i >= 0.000 & pbs_i < 0.200, "Tolerable", "Good")))),
         dist_p_flag = ifelse(n_i<=1, NA,
                              ifelse(response <= n_option & response != Item_key & (p < 0.020 | is.na(p)==T), 1, 0)),
         dist_pbs_flag = ifelse(n_i<=1, NA,
                                ifelse(response <= n_option & response != Item_key & pbs > 0.050 , 1, 0)),
         dist_key_flag = ifelse(n_i<=1, NA,
                                ifelse(response <= n_option & response != Item_key & pbs > pbs_i, 1, 0))) %>% 
  mutate(dist_p_flag = ifelse(sum(dist_p_flag, na.rm = T) >=1, "option p < 2%", NA),
         dist_pbs_flag = ifelse(sum(dist_pbs_flag, na.rm = T) >=1, "distractor pbs > 0.05", NA),
         dist_key_flag = ifelse(sum(dist_key_flag, na.rm = T) >=1, "distractor pbs > key pbs", NA)) %>%
  ungroup() %>% 
  pivot_wider(names_from = response, values_from = c(n,p,pbs)) %>% 
  rename("n"="n_i", "pbs"="pbs_i", "pbs_c"="pbs_c_i") %>% 
  relocate(c(Low_n_flag:dist_key_flag), .after = last_col()) %>% 
  mutate(n_flag = rowSums(!is.na(select(., c(Low_n_flag,dist_p_flag:dist_key_flag))))) %>% 
  mutate(n_flag = ifelse(pvalue_flag %in% c("Too Easy","Too Hard"), n_flag+1, n_flag)) %>% 
  mutate(n_flag = ifelse(pbs_flag=="Unacceptable", n_flag+1, n_flag)) 


# Output.
final_out %>% View()

# Export (temp).
# writexl::write_xlsx(final_out, paste0("G:\\My Drive\\0. Projects\\IA Standardization\\Sample output\\",str_extract(master[1], "([^_]*)"),str_extract(master[1], "_([^_]*)"),"_IA_output.xlsx"))
#
