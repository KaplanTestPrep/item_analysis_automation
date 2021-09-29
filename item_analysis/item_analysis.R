
# Empty environment.
rm(list = ls())

## Enter/Assign data folder path.
fpath <- "..."


## Sample data to test the code
# fpath <- "G:\\Shared drives\\Psychometrics\\KNA LAP\\_Programs\\MCAT\\Item Analysis\\2021_foundational_course\\data\\BioChem"
# fpath <- "G:\\Shared drives\\Psychometrics\\KNA LAP\\_Programs\\MCAT\\Item Analysis\\2021_foundational_course\\data\\BehSci"
# fpath <- "G:\\Shared drives\\Psychometrics\\KNA LAP\\_Programs\\Bar\\Item Analysis\\2021 Diag Pilot Test\\Data\\cleaned_data" <<<<<<<<<<<


# TO BE RESOLVED: <<<<<<<<<<<
## -input data files: how do we plan on communicating cleaning code and analysis code?
### current IA code below reads in all data files from the cleaned data folder, zipped folder would work better.
## -column name for student ID differs across files.
## -differing file structures:
### column names can differ: sequenceName vs. template_name, contentItemName vs. content_item_id, correctAnswer vs. correct_answer
### items showing in multiple templates: combine responses ???
### los_count: where do we get this information?
### skill_level, Topic_1, Topic_2: may not be found in all data files.
## response categories: are these always {1,2,3,4}?
## to where and in which format to export the output?



# ITEM ANALYSIS AUTOMATION.
# Load packages.
library(dplyr)
library(stringr)
library(magrittr)
library(tidyr)
library(readr)

# Read in cleaned item response data.
## read file names in the folder.
master <- list.files(fpath)
## remove if there are other types in the folder. <<<<<<<<<<<
master <- master[str_detect(master, ".csv")]
## load data files into a list.
dflist <- list()
for (i in 1:length(master)) {
  tt <- read_csv(paste0(fpath,"\\",master[i]),
                 col_names = T, 
                 na = c( "-99", NA, "NA", ".")) %>% as.data.frame()
  dflist[[master[i]]] <- tt
}
## rename data frames.
names(dflist)[which(str_detect(names(dflist), "User_level_Item_Scores"))] <- "scored"
names(dflist)[which(str_detect(names(dflist), "User_level_Responses"))] <- "response_data"
names(dflist)[which(str_detect(names(dflist), "Content_Item_Info"))] <- "content_info"
names(dflist)[which(str_detect(names(dflist), "Sequence_Level_Info"))] <- "seq_info"
# Match column name for student ID across files. <<<<<<<<<<<
names(dflist$response_data)[1] <- "ID"
names(dflist$scored)[1] <- "ID"
names(dflist$seq_info)[1] <- "ID"



# CTT analysis.
out_all <- data.frame()
for (i in 2:ncol(dflist$response_data)) {
  out_i <- data.frame()
  name_i <- names(dflist$response_data)[i]
  
  # if an item shows up in more than one??? <<<<<<<<<<<
  if(length(dflist$content_info$template_name[which(dflist$content_info$content_item_name==name_i)]) > 1){
    
  }
  
  # data files with different formats/structures <<<<<<<<<<<
  if("contentItemName" %in% names(dflist$content_info)){
    for (k in levels(as.factor(dflist$response_data[,i]))) {
      out_ik <- data.frame("Test_name"=dflist$content_info$sequenceName[which(dflist$content_info$contentItemName==name_i)],
                           "Test_ID"=dflist$content_info$templateId[which(dflist$content_info$contentItemName==name_i)],
                           "Item_name"=name_i,
                           "QID"=dflist$content_info$contentItemId[which(dflist$content_info$contentItemName==name_i)],
                           # "los_count"=, <<<<<<<<<<<
                           "parent_ID"=dflist$content_info$parentid[which(dflist$content_info$contentItemName==name_i)],
                           "Item_key"=dflist$content_info$correctAnswer[which(dflist$content_info$contentItemName==name_i)],
                           "n_option"=dflist$content_info$countchoices[which(dflist$content_info$contentItemName==name_i)],
                           # "skill_level"=, <<<<<<<<<<<
                           # "Topic_1"=, <<<<<<<<<<<
                           # "Topic_2"=, <<<<<<<<<<<
                           "n_i"=dflist$content_info$count_seen[which(dflist$content_info$contentItemName==name_i)],
                           "n_omit"=dflist$content_info$count_seen[which(dflist$content_info$contentItemName==name_i)] - dflist$content_info$count_att[which(dflist$content_info$contentItemName==name_i)],
                           "n_correct"=dflist$content_info$num_correct[which(dflist$content_info$contentItemName==name_i)],
                           "pvalue"=dflist$content_info$num_correct[which(dflist$content_info$contentItemName==name_i)]/
                             dflist$content_info$count_att[which(dflist$content_info$contentItemName==name_i)],
                           "pbs_i"=dflist$scored %>% 
                             mutate(raw_correct = rowSums(.[-1], na.rm = T)) %$%
                             cor(raw_correct, .[,i], use = "complete.obs"),
                           "pbs_c_i"=dflist$scored %>% 
                             mutate(raw_correct = rowSums(.[-c(1,i)], na.rm = T)) %$%
                             cor(raw_correct, .[,i], use = "complete.obs"),
                           "p_omit"=(dflist$content_info$count_seen[which(dflist$content_info$contentItemName==name_i)] - dflist$content_info$count_att[which(dflist$content_info$contentItemName==name_i)]) /
                             dflist$content_info$count_seen[which(dflist$content_info$contentItemName==name_i)],
                           "response"=k,
                           "n"=NROW(dflist$response_data[,i][which(dflist$response_data[,i]==k)]),
                           "p"=NROW(dflist$response_data[,i][which(dflist$response_data[,i]==k)])/
                             NROW(dflist$response_data[,i][which(is.na(dflist$response_data[,i])==F)]),
                           "pbs"=dflist$response_data %>% 
                             merge(dflist$seq_info %>% select(ID, template_raw_correct), by = "ID") %>% 
                             mutate(binary = ifelse(dflist$response_data[,i]==k,1,0)) %$%
                             cor(template_raw_correct, binary, use = "complete.obs")
      )
      out_i <- rbind(out_i, out_ik)
    }
    
  }else{
    for (k in levels(as.factor(dflist$response_data[,i]))) {
      out_ik <- data.frame("Test_name"=dflist$content_info$template_name[which(dflist$content_info$content_item_name==name_i)],
                           "Test_ID"=dflist$content_info$template_id[which(dflist$content_info$content_item_name==name_i)],
                           "Item_name"=name_i,
                           "QID"=dflist$content_info$content_item_id[which(dflist$content_info$content_item_name==name_i)],
                           # "los_count"=, <<<<<<<<<<<
                           "parent_ID"=dflist$content_info$parent_item_id[which(dflist$content_info$content_item_name==name_i)],
                           "Item_key"=dflist$content_info$correct_answer[which(dflist$content_info$content_item_name==name_i)],
                           "n_option"=dflist$content_info$count_choices[which(dflist$content_info$content_item_name==name_i)],
                           # "skill_level"=dflist$content_info$difficultylevel[which(dflist$content_info$content_item_name==name_i)], <<<<<<<<<<<
                           # "Topic_1"=dflist$content_info$l2title[which(dflist$content_info$content_item_name==name_i)], <<<<<<<<<<<
                           # "Topic_2"=dflist$content_info$l3title[which(dflist$content_info$content_item_name==name_i)], <<<<<<<<<<<
                           "n_i"=dflist$content_info$count_att[which(dflist$content_info$content_item_id==name_i)],
                           "n_omit"=dflist$content_info$count_seen[which(dflist$content_info$content_item_id==name_i)] - dflist$content_info$count_att[which(dflist$content_info$content_item_id==name_i)],
                           "n_correct"=dflist$content_info$num_correct[which(dflist$content_info$content_item_id==name_i)],
                           "pvalue"=dflist$content_info$num_correct[which(dflist$content_info$content_item_id==name_i)]/
                             dflist$content_info$count_att[which(dflist$content_info$content_item_id==name_i)],
                           "pbs_i"=dflist$scored %>% 
                             mutate(raw_correct = rowSums(.[-1], na.rm = T)) %$%
                             cor(raw_correct, .[,i], use = "complete.obs"),
                           "pbs_c_i"=dflist$scored %>% 
                             mutate(raw_correct = rowSums(.[-c(1,i)], na.rm = T)) %$%
                             cor(raw_correct, .[,i], use = "complete.obs"),
                           "p_omit"=(dflist$content_info$count_seen[which(dflist$content_info$content_item_id==name_i)] - dflist$content_info$count_att[which(dflist$content_info$content_item_id==name_i)]) /
                             dflist$content_info$count_seen[which(dflist$content_info$content_item_id==name_i)],
                           "response"=k,
                           "n"=NROW(dflist$response_data[,i][which(dflist$response_data[,i]==k)]),
                           "p"=NROW(dflist$response_data[,i][which(dflist$response_data[,i]==k)])/
                             NROW(dflist$response_data[,i][which(is.na(dflist$response_data[,i])==F)]),
                           "pbs"=dflist$response_data %>% 
                             merge(dflist$seq_info %>% select(ID, template_raw_correct), 
                                   by = "ID") %>% 
                             mutate(binary = ifelse(dflist$response_data[,i]==k,1,0)) %$%
                             cor(template_raw_correct, binary, use = "complete.obs")
      )
      out_i <- rbind(out_i, out_ik)
    }
  }
  
  out_all <- rbind(out_all, out_i)
}

# Flags
final_out <- out_all %>% 
  mutate(across(c(pvalue,pbs_i,pbs_c_i,p_omit,p,pbs), round, 2)) %>%
  group_by(Item_name) %>% 
  mutate(Low_n_flag = ifelse(n_i < 50, "n < 50", NA),
         pvalue_flag = ifelse(pvalue > 0.90, "Too Easy",
                              ifelse(pvalue < 0.25, " Too Hard", "Acceptable")),
         pbs_flag = ifelse(pbs_i >= 0.30, "Excellent",
                           ifelse(pbs_i < 0.00, "Unacceptable",
                                  ifelse(pbs_i >= 0.00 & pbs_i < 0.20, "Tolerable", "Good"))),
         dist_p_flag = ifelse(response != Item_key & p < 0.02, 1, 0),
         dist_pbs_flag = ifelse(response != Item_key & pbs > 0.05, 1, 0),
         dist_key_flag = ifelse(response != Item_key & pbs > pbs_i, 1, 0)) %>% 
  mutate(dist_p_flag = ifelse(sum(dist_p_flag) >=1, "option p < 2%", NA),
         dist_pbs_flag = ifelse(sum(dist_pbs_flag) >=1, "distractor pbs > 0.05", NA),
         dist_key_flag = ifelse(sum(dist_key_flag) >=1, "distractor pbs > key pbs", NA)) %>%
  ungroup() %>% 
  pivot_wider(names_from = response, values_from = c(n,p,pbs)) %>% 
  relocate(c(n_1:pbs_4), .before = "Low_n_flag") %>% 
  rename("n"="n_i", "pbs"="pbs_i", "pbs_c"="pbs_c_i") %>% 
  mutate(across(c(pvalue,pbs,pbs_c,p_omit,p_1:p_4,pbs_1:pbs_4), format, digits=2))

# Output.
final_out %>% View()

# Export (temp). <<<<<<<<<<<
# writexl::write_xlsx(final_out, paste0("G:\\My Drive\\0. Projects\\IA Standardization\\Sample output\\",str_extract(master[1], "([^_]*)"),str_extract(master[1], "_([^_]*)"),"_IA_output.xlsx"))
#



