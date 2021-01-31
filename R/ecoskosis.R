#' R Package for scraping data from ECOS and KOSIS
#' k_get_date_list
#'
#' @author Junah Bang \email{juna0033@@g.skku.edu}
#' @param start_date
#' @param end_date
#' @md
#' @export
k_get_date_list <- function(start_date, end_date){
  s_year <- start_date %>% str_sub(1, 4) %>% as.numeric()
  s_mon <- start_date %>% str_sub(5, 6) %>% as.numeric()
  e_year <- end_date %>% str_sub(1, 4) %>% as.numeric()
  e_mon <- end_date %>% str_sub(5, 6) %>% as.numeric()
  if(s_year == e_year){
    date_list <- paste0(s_year, str_pad(s_mon:e_mon, 2, side = "left", pad = 0))
  }else if(e_year - s_year == 1){
    date_list <- paste0(s_year, str_pad(s_mon:12, 2, side = "left", pad = 0)) %>%
      c(paste0(e_year, str_pad(1:e_mon, 2, side = "left", pad = 0)))
  }else{
    whole_mon <- NULL
    for(y_ind in seq(e_year - s_year - 1)){
      whole_mon <- whole_mon %>%
        c(paste0(s_year + y_ind, str_pad(1:12, 2, side = "left", pad = 0)))
    }
    date_list <- paste0(s_year, str_pad(s_mon:12, 2, side = "left", pad = 0)) %>%
      c(whole_mon) %>%
      c(paste0(e_year, str_pad(1:e_mon, 2, side = "left", pad = 0)))
  }
  return(date_list)
}

#' get kosis url
#' @param api_key
#' @param target_date
#' @param url
#' @md
#' @export
k_get_url <- function(api_key, target_date, url){
  # api key 변경
  url <- url %>%
    str_replace_all("NDRjYzg1NGY2MmI1M2ZiOTU1MGU4ZTUzY2M3NWQ4N2Q=", api_key)
  # 기존 startPrdDe/endPrdDe 위치 잡아서 target_date로 변경
  s_date_lower <- str_locate(url, "&startPrdDe=")[ ,2]
  s_date_upper <- str_locate(url, "&endPrdDe=")[ ,1]
  e_date_lower <- str_locate(url, "&endPrdDe=")[ ,2]
  e_date_upper <- str_locate(url, "&loadGubun=")[ ,1]
  str_sub(url, start = s_date_lower + 1, end = s_date_upper - 1) <- target_date
  str_sub(url, start = e_date_lower + 1, end = e_date_upper - 1) <- target_date
  return(url)
}

#' get api db from kosis; one month
#' @param api_key
#' @param date
#' @param stat_id_list
#' @md
#' @export
k_get_api_db_one_mon <- function(api_key, date, stat_id_list){
  k_target <- kosis_stat_nm_tbl %>%
    filter(stat_id %in% stat_id_list) %>%
    mutate(url = url %>%
             k_get_url(api_key = api_key, target_date = date))
  k_output <- NULL
  for(tbl_ind in seq_along(k_target$url)){
    json_temp <- k_target$url[tbl_ind]%>% GET() %>%
      content(as = "text", encoding = "UTF-8")
    k_output[[k_target$RF_YN[tbl_ind]]] <-
      tryCatch(fromJSON(txt = json_temp %>% as.character()) %>%
                 as_tibble() %>%
                 spread(key = PRD_DE, value = DT) %>%
                 select(-ends_with("_ENG"), -TBL_NM, -PRD_SE, -ORG_ID),
               error =
                 function(e) {k_output[[k_target$RF_YN[tbl_ind]]] = as_tibble(data.frame("TBL_ID" = "Error", "ITM_NM" = json_temp %>% as.character() %>% str_replace_all('["\a-z{}]', ''), stringsAsFactors=FALSE))})
  }
  out_tbl <- k_output %>%
    bind_rows(.id="RF_YN")
  return(out_tbl)
}

#' get api db from kosis; fot target period
#' @param api_key
#' @param start_date
#' @param end_date
#' @param stat_id_list
#' @md
#' @export
kosis_get_api_db <- function(api_key, start_date, end_date, stat_id_list){
  time.started <- Sys.time()
  cat(paste('Started at : ', time.started, ' / ...ing...', sep = ''))
  date_list <- k_get_date_list(start_date, end_date)
  k_api_out <- NULL
  for(mon_ind in seq_along(date_list)){
    k_api_out[[date_list[mon_ind]]] <-
      k_get_api_db_one_mon(api_key, date_list[mon_ind], stat_id_list)
  }
  k_api_out_tbl <- k_api_out %>%
    reduce(full_join)
  time.finished <- Sys.time() # Store finished time
  time.elapsed  <- time.finished - time.started # Calculate elapsed time
  cat(paste('Finished..! / elapsed time : ', time.elapsed, '\n\n', sep = ''))
  return(k_api_out_tbl)
}

#' ECOS
#' convert_xml_to_tbl
#' @param api_url
#' @md
#' @export
e_convert_xml_to_tbl <- function(api_url){
  api_xml <- xmlTreeParse(api_url, useInternalNodes = TRUE) %>%
    xmlRoot()
  output_tbl <- NULL
  for(ind in seq(xmlSize(api_xml))){
    if(api_xml[[ind]] %>% xmlSize != 1){
      api_item <- xmlSApply(api_xml[[ind]], xmlValue)
      obs_line <- api_item %>% t() %>% as_tibble
      data.frame(stringsAsFactors=FALSE)
      output_tbl <- output_tbl %>%
        rbind(obs_line)
    }
  }
  output_tbl %>% as_tibble()
}

#' ECOS
#' e_get_url
#' @param stat_code
#' @param ecos_api_key
#' @param start_date
#' @param end_date
#' @md
#' @export
e_get_url <- function(stat_code, ecos_api_key, start_date, end_date){
  # 2.6. table의 경우 월단위로 변경 필요
  ecos_stat_tbl <- ecos_target_stat %>%  #ecos_target_stat_updated %>%
    mutate(CYCLE = ifelse(STAT_CODE == "098Y001", "MM", CYCLE))  %>%
    filter(RF_YN != "E131")
  ecos_base_url <- "http://ecos.bok.or.kr/api/"
  ecos_target <- "StatisticSearch/"#"StatisticItemList/" #"StatisticSearch/"
  ecos_sub_url <- "/xml/kr/"
  start_count <- "1"
  end_count <- "30000"
  cycle_type <- ecos_stat_tbl[ecos_stat_tbl$STAT_CODE == stat_code, ]$CYCLE
  if(cycle_type == "YY"){
    start_date <- start_date %>% str_sub(start = 1, end = 4) %>% as.numeric() - 1
    end_date <- end_date %>% str_sub(start = 1, end = 4) %>% as.numeric() - 1
  }else if(cycle_type == "QQ"){
    start_y <- start_date %>% str_sub(start = 1, end = 4) %>% as.numeric()
    start_m <- start_date %>% str_sub(start = 5, end = 6) %>% as.numeric()
    end_y <- end_date %>% str_sub(start = 1, end = 4) %>% as.numeric()
    end_m <- end_date %>% str_sub(start = 5, end = 6) %>% as.numeric()
    if(start_m %in% 1:3){
      start_date <- paste0(start_y - 1, 4)
    }else if(start_m %in% 4:6){
      start_date <- paste0(start_y, 1)
    }else if(start_m %in% 7:9){
      start_date <- paste0(start_y, 2)
    }else{
      start_date <- paste0(start_y, 3)
    }
    if(end_m %in% 1:3){
      end_date <- paste0(end_y - 1, 4)
    }else if(end_m %in% 4:6){
      end_date <- paste0(end_y, 1)
    }else if(end_m %in% 7:9){
      end_date <- paste0(end_y, 2)
    }else{
      end_date <- paste0(end_y, 3)
    }
  }
  output_url <- paste0(ecos_base_url,
                       ecos_target,
                       ecos_api_key, ecos_sub_url,
                       start_count, "/", end_count, "/",
                       stat_code,
                       "/", cycle_type, "/",
                       start_date, "/", end_date)
  return(output_url)
}

#' get api db from ECOS
#' @param stat_code
#' @param ecos_api_key
#' @param start_date
#' @param end_date
#' @md
#' @export
ecos_get_api_db <- function(ecos_api_key, start_date, end_date, stat_code_list){
  time.started <- Sys.time()
  cat(paste('Started at : ', time.started, ' / ...ing...', sep = ''))
  target_stat <- ecos_target_stat %>%
    filter(RF_YN != "E131") %>%
    filter(STAT_CODE %in% stat_code_list)
  output <- NULL
  for(stat_ind in seq_along(target_stat$STAT_CODE)){
    output[[target_stat$RF_YN[stat_ind]]] <-
      tryCatch(target_stat$STAT_CODE[stat_ind] %>%
                 e_get_url(ecos_api_key, start_date, end_date) %>%
                 e_convert_xml_to_tbl() %>%
                 spread(key = TIME, value = DATA_VALUE),
               error = function(e) {output[[target_stat$RF_YN[stat_ind]]] =
                 as_tibble(data.frame("STAT_CODE" = "Error",
                                      stringsAsFactors=FALSE))})
  }
  time.finished <- Sys.time() # Store finished time
  time.elapsed  <- time.finished - time.started # Calculate elapsed time
  cat(paste('Finished..! / elapsed time : ', time.elapsed, '\n\n', sep = ''))
  output_tbl <- output %>%
    bind_rows(.id = "RF_YN")
  return(output_tbl)
}

#' function to convert to 6months format
#' @md
#' @export
e_handle_api_db <- function(api_db_tbl){
  in_tbl <- api_db_tbl %>%
    left_join(ecos_target_stat %>% select(RF_YN, CYCLE), by = "RF_YN")
  date_vec <- in_tbl %>% select(starts_with("20")) %>% names()
  mon_vec <- date_vec[str_length(date_vec) == "6"]
  year_vec <- date_vec[str_length(date_vec) == "4"]
  quarter_vec <- date_vec[str_length(date_vec) == "5"]
  ref_tbl <- data.frame(MM = date_vec[str_length(date_vec) == "6"], stringsAsFactors=FALSE) %>%
    as_tibble() %>%
    mutate(YY = MM %>% str_sub(1, 4) %>% as.numeric() - 1) %>%
    mutate(QQ = ifelse(as.numeric(str_sub(MM, 5, 6)) %in% c(1:3),
                       paste0(MM %>% str_sub(1, 4) %>% as.numeric() - 1, 4),
                       ifelse(as.numeric(str_sub(MM, 5, 6)) %in% c(4:6),
                              paste0(MM %>% str_sub(1, 4), 1),
                              ifelse(as.numeric(str_sub(MM, 5, 6)) %in% c(7:9),
                                     paste0(MM %>% str_sub(1, 4), 2),
                                     ifelse(as.numeric(str_sub(MM, 5, 6)) %in% c(10:12),
                                            paste0(MM %>% str_sub(1, 4), 3), "NON"))))) %>%
    mutate(DB_DATE = MM) %>%
    gather(key = CYCLE, value = API_DATE, MM, YY, QQ)
  out_tbl <- in_tbl %>% # in_tbl
    gather(key = API_DATE, value = VALUE, date_vec ) %>%  #all_of(date_vec)
    filter(!is.na(VALUE)) %>%
    left_join(ref_tbl,
              by = c("API_DATE", "CYCLE")) %>%
    select(-API_DATE) %>%
    spread(key = DB_DATE, value = VALUE) %>%
    mutate(RF_YN = ifelse(ITEM_NAME1 == "실질" & RF_YN == "E130", "E131", RF_YN))
  out_tbl[out_tbl$ITEM_CODE2 == " ", ]$ITEM_CODE2 <- NA
  out_tbl[out_tbl$ITEM_CODE3 == " ", ]$ITEM_CODE3 <- NA
  start_date <- date_vec[str_length(date_vec) == "6"] %>% as.numeric() %>% min()
  end_date <- date_vec[str_length(date_vec) == "6"] %>% as.numeric() %>% max()
  cat(paste('Finished..! / start date : ', start_date, '\t',
            'end date : ', end_date, sep = ' ', '\n\n'))
  return(out_tbl)
}
