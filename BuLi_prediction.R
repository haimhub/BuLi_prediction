##########################################################
##########################################################
# Football results prediction - German Bundesliga - remaining games after COVID19 related lockdown
# (HarvardX project submission - PH125.9x Data Science: Capstone)
# Author: Martin Haitzmann
# eMail: martin.c.haitzmann@gmail.com
# https://github.com/haimhub/BuLi_prediction

##########################################################
# Set overall parameters and load libraries
##########################################################
# gc(rm(list = ls()))

# relative path stored data
rel_data_dir <- "./data"

# load/install relevant libraries
if (!require(tidyverse)) install.packages("tidyverse", repos = "http://cran.us.r-project.org")
if (!require(caret)) install.packages("caret", repos = "http://cran.us.r-project.org")
if (!require(data.table)) install.packages("data.table", repos = "http://cran.us.r-project.org")
if (!require(DBI)) install.packages("DBI", repos = "http://cran.us.r-project.org")
if (!require(RSQLite)) install.packages("RSQLite", repos = "http://cran.us.r-project.org")
if (!require(ggrepel)) install.packages("ggrepel", repos = "http://cran.us.r-project.org")
if (!require(devtools)) install.packages("devtools", repos = "http://cran.us.r-project.org")
if (!require(regista)) devtools::install_github("torvaney/regista")

# seed for reproducibility
set.seed(10, sample.kind = "Rounding")

##########################################################
# Download data
##########################################################

# Download basic database from own github or database from
# https://github.com/openfootball/deutschland
#
DL_flat_file_fromGit <- TRUE

if (DL_flat_file_fromGit) {
  temp <- tempfile()
  urlfile <- "https://raw.githubusercontent.com/haimhub/BuLi_prediction/master/data/matches_all.csv"
  download.file(urlfile,
    destfile = temp
  )
  matches_all <- read_csv(temp)
  unlink(temp)
} else {
  # Download from openfootball (dl manually in case of problems with malformed DB )
  db_dl <- "v2020.07.09/deutschland.db"
  sapply(db_dl, function(x) {
    download.file(
      paste0("https://github.com/openfootball/", gsub(".*/|\\.db", "", db_dl), "/releases/download/", db_dl),
      file.path(rel_data_dir, gsub(".*/", "", db_dl))
    )
  })

  # Extract relevant games from db
  #####################################

  # establish database connection
  filename <- file.path(rel_data_dir, "deutschland.db")
  db <- DBI::dbConnect(RSQLite::SQLite(),
    dbname = filename
  )

  ## check the database structure showing the first entries of each table
  insp_db <- sapply(DBI::dbListTables(db), function(x) if (nrow(dbReadTable(db, x)) > 0) head(dbReadTable(db, x), 1))

  # sql query for data extration
  query.text <- paste0(
    "SELECT t1.key as team1, t2.key as team2, e.key as event_k, s.name as season, r.name as round_name, g.*  FROM matches g ",
    "INNER JOIN teams t1 ON t1.id = g.team1_id ",
    "INNER JOIN teams t2 ON t2.id = g.team2_id ",
    "INNER JOIN rounds r ON r.id = g.round_id ",
    "INNER JOIN events e ON e.id = r.event_id ",
    "INNER JOIN seasons s ON s.id = e.season_id ",
    "WHERE e.key LIKE 'de.1.20%/%'"
  )

  matches_all <- DBI::dbGetQuery(
    db, query.text
  )
  DBI::dbDisconnect(db)
  write.csv(matches_all, file.path(rel_data_dir, "matches_all.csv"), row.names = FALSE)
}

##########################################################
# Further preprocess data
##########################################################

# extracted data as data.frame/tibble
matches_all <- matches_all %>%
  select_if(~ sum(!is.na(.)) > 0) %>%
  select(-c(created_at, updated_at))
matches_all$round <- as.numeric(gsub("Spieltag ", "", matches_all$round_name))
#####################################
#####################################
# date and round number when league was suspended due to pandemic
stop_date <- "2020-03-13"
cut_round <- 25
####
# matches_all$bef_25 <- ifelse(matches_all$round <= cut_round, TRUE, FALSE)

# define variables that are necessary for further use
rel_vars <- c(
  "team1", "team2",
  "score1", "score2",
  "round", "season",
  "date", "winner"
)

rel_vars_txt <- c(
  "home", "away",
  "goals home team", "goals away team",
  "round", "season",
  "date game played", "winner"
)

# matches in relevant season:  set scores NA for games after lockdown
results_all <- matches_all[, rel_vars]
results_past <- matches_all[matches_all$date < stop_date, rel_vars]

# current season split into games with results and NA after lockdown
fixt <- matches_all[matches_all$season == "2019/20", rel_vars]
fixt_open_results <- fixt[fixt$date > stop_date, ]
fixt[fixt$date > stop_date, c("score1", "score2", "winner")] <- NA

# create own df for played and open fixtures
fixt_played <- fixt %>% filter(!is.na(score1))
fixt_open <- fixt %>% filter(is.na(score1))

# extract teams as vector
# teams <- unique(fixt$team1)
###################################
pos.outcomes <- data.frame(
  num = c(0, 1, 2),
  char = c("draw", "win_team1", "win_team2"),
  desc = c("Draw (both teams same score)", "Team1 wins (home win)", "Team2 wins (away win)"),
  stringsAsFactors = FALSE
)

# create vectro used in analysis and model building covering different seasons
rel_seasons_long <- c("2013/14", "2014/15", "2015/16", "2016/17", "2017/18", "2018/19", "2019/20")
rel_seasons_short <- c("2017/18", "2018/19", "2019/20")

rel_season_modelbuild <- c("2015/16", "2016/17", "2017/18", "2018/19")
rel_season_modelfinal <- c("2016/17", "2017/18", "2018/19", "2019/20")
##

#########################################
# Create functions that are used more often in data exploration
# as to data exloration and model development see BuLi_prediction.Rmd
#########################################
# function to melt results
# returns df with team and goals for and against for each match
melt_results <- function(results_df) {
  results_df %>%
    # select only relevant columns
    select(team1, team2, score1, score2, round, season) %>%
    gather(loc, team, -score1, -score2, -round, -season) %>%
    # calculate goals for/against the team
    mutate(g_for = case_when(
      loc == "team1" ~ score1,
      loc == "team2" ~ score2
    )) %>%
    mutate(g_ag = case_when(
      loc == "team1" ~ score2,
      loc == "team2" ~ score1
    )) %>%
    mutate(points = case_when(
      g_for > g_ag ~ 3,
      g_for < g_ag ~ 0,
      g_for == g_ag ~ 1
    )) %>%
    select(-score1, -score2)
}

# function to calculate points won and gd for each team
results_to_table <- function(results_df) {
  results_df %>%
    # use above melting function
    melt_results(.) %>%
    # 3 points for a win, 1 for a draw
    mutate(points = case_when(
      g_for > g_ag ~ 3,
      g_ag > g_for ~ 0,
      g_for == g_ag ~ 1
    )) %>%
    # calculate goal difference for each match
    mutate(
      gd = g_for - g_ag,
      win = ifelse(gd > 0, 1, 0),
      draw = ifelse(gd == 0, 1, 0),
      lose = ifelse(gd < 0, 1, 0)
    ) %>%
    group_by(team) %>%
    # get the final statistics per team
    summarise(
      games_played = n(),
      wins = sum(win),
      draws = sum(draw),
      lost = sum(lose),
      g_scored = sum(g_for),
      g_received = sum(g_ag),
      g_diff = sum(gd),
      points = sum(points)
    ) %>%
    arrange(-points, -g_diff, -g_scored)
}


# change prediction to use log parameters
# exp(log(x) + log(y)) = x * y
predict_results <- function(team1, team2, param_list) {
  e_goals_team1 <- exp(param_list$alpha[team1] - param_list$beta[team2] + param_list$gamma)
  e_goals_team2 <- exp(param_list$alpha[team2] - param_list$beta[team1])
  df <- data.frame(
    team1 = team1, team2 = team2,
    e_score1 = e_goals_team1, e_score2 = e_goals_team2
  )
  return(df)
}

optimise_params <- function(parameters, results = train_set) {
  # form the parameters back into a list
  # parameters names alpha (attack), beta (defense), and gamma (hfa)
  param_list <- relist_params(parameters)

  # predict the expected results for the games that have been played
  e_results <- map2_df(
    results$team1, results$team2,
    predict_results,
    param_list
  )

  # calculate the negative log likelihood of those predictions
  # given the parameters how likely are those scores
  neg_log_likelihood <- calculate_log_likelihood(results, e_results)

  # capture the parameters and likelihood at each loop
  # only do it if i is initialised
  if (exists("i")) {
    i <<- i + 1
    current_parameters[[i]] <<- parameters
    current_nll[[i]] <<- neg_log_likelihood
  }

  # return the value to be minimised
  # in this case the negative log likelihood
  return(neg_log_likelihood)
}

# optim requires parameters to be supplied as a vector
# we'll unlist the parameters then relist in the function
# introduce sum to zero constraint by calculating
# first teams parameters as minus sum of the rest
relist_params <- function(parameters) {
  parameter_list <- list(
    alpha = parameters %>%
      .[grepl("alpha", names(.))] %>%
      append(prod(sum(.), -1), .) %>%
      `names<-`(rel_teams),
    beta = parameters %>%
      .[grepl("beta", names(.))] %>%
      append(prod(sum(.), -1), .) %>%
      `names<-`(rel_teams),
    gamma = parameters["gamma"]
  )

  return(parameter_list)
}


# calculate the log likelihood of predict results vs supplied results
calculate_log_likelihood <- function(results, e_results) {
  home_likelihoods <- dpois(results$score1, lambda = e_results$e_score1, log = TRUE)
  away_likelihoods <- dpois(results$score2, lambda = e_results$e_score2, log = TRUE)

  # sum log likelihood and multiply by -1 so we're minimising neg log likelihood
  likelihood_sum <- sum(home_likelihoods, away_likelihoods)
  neg_log_likelihood <- prod(likelihood_sum, -1)

  return(neg_log_likelihood)
}


#########################################
# Explore data and develop possible models
#########################################

# Data exploration and model development done in Rmarkdown file:

# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# ./BuLi_prediction.Rmd
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#

#########################################
# Results: Final Model
#########################################

# decide for the final modelling approach to use
# dixoncoles model based on poisson distribution
# model development in Rmd shows how to approach this model
# however, there is also an implementaion in the regista package that is by far faster
# model final result with this package
# run simulation of results several times to get prob distributions of final result

call_final_model <- TRUE

if (call_final_model) {
  # set number of simulation runs
  # set to 10000 runs, although might quite run some time
  n.simrun <- 10000
  ##
  show_plots <- FALSE


  # calculate the relevant table standings at suspension of league
  fit_final_cur_table <- results_to_table(fixt_played) %>% select(team, points, g_diff)

  # calculate the final table after open games were played in real world to compare results from simulation
  final_table_real <- results_to_table(rbind(fixt_played, fixt_open_results)) %>%
    rownames_to_column("final_rank")

  ############################
  # define train and test set
  ############################
  test_set <- fixt_open_results
  rel_teams <- unique(test_set$team1)

  train_set <- results_past %>% filter(season %in% rel_season_modelfinal &
    (team1 %in% rel_teams & team2 %in% rel_teams))

  train_set$winner <- as.factor(train_set$winner)
  test_set$winner <- as.factor(test_set$winner)

  ############################
  # fit_final model
  ############################
  fit_final <- regista::dixoncoles(score1, score2, as.factor(team1), as.factor(team2), data = train_set)

  # calculate confuson matrix
  ######
  fit_final_y <- lapply(
    predict(fit_final, newdata = test_set, type = "outcomes"),
    function(x) as.character(x[which(x[, 2] == max(x[, 2])), "outcome"])
  ) %>%
    unlist()
  fit_final_y <- ifelse(fit_final_y == "home_win", 1, ifelse(fit_final_y == "away_win", 2, 0))

  fit_final_CM <- confusionMatrix(as.factor(fit_final_y), as.factor(test_set$winner))
  ######

  # extract team strenth parameters from model
  pars <- fit_final$par %>%
    .[grepl("def_|off_", names(.))] %>%
    matrix(., ncol = 2) %>%
    as.data.frame() %>%
    rename(attack = V1, defence = V2)
  pars$team <- unique(gsub("def_*|off_*", "", names(fit_final$par)))[-grep("^hfa$|^rho", unique(gsub("def_*|off_*", "", names(fit_final$par))))]

  # plot with optimized attack and defense parameter per team
  p_final_att_def <- pars %>%
    mutate(defence = 1 - defence) %>%
    ggplot(aes(x = attack, y = defence, colour = attack + defence, label = team)) +
    geom_point(size = 3, alpha = 0.7) +
    geom_text_repel() +
    scale_colour_continuous(guide = FALSE) +
    labs(
      title = "Dixon-Coles parameters",
      x = "attacking strength",
      y = "defensive strength"
    ) +
    theme_minimal()

  if (show_plots) p_final_att_def

  ############################
  # run many simulations on the model to get distribution of possible final outcomes
  ############################
  # simulate remaining matches
  fit_final_sims <- rerun(
    n.simrun,
    augment.dixoncoles(fit_final, test_set, type.predict = "scorelines") %>%
      mutate(sampled_result = map(.scorelines, sample_n, 1, weight = prob)) %>%
      select(-.scorelines) %>%
      unnest(cols = c(sampled_result)) %>%
      pivot_longer(c(team1, team2), names_to = "loc", values_to = "team") %>%
      mutate(points = case_when(
        loc == "team1" & hgoal > agoal ~ 3,
        loc == "team2" & agoal > hgoal ~ 3,
        hgoal == agoal ~ 1,
        TRUE ~ 0
      )) %>%
      mutate(g_diff = case_when(
        loc == "team1" ~ hgoal - agoal,
        loc == "team2" ~ agoal - hgoal
      )) %>%
      select(team, points, g_diff)
  )

  # calculate final tables for each run of simulation
  fit_final_predicted_finishes <- map_df(fit_final_sims, function(sim_fixtures, table) {
    sim_fixtures %>%
      select(team, points, g_diff) %>%
      bind_rows(., table) %>%
      group_by(team) %>%
      summarise(
        points = sum(points),
        g_diff = sum(g_diff)
      ) %>%
      arrange(-points, -g_diff) %>%
      mutate(predicted_finish = 1:n())
  }, fit_final_cur_table) %>%
    group_by(team, predicted_finish) %>%
    summarise(perc = n() / n.simrun) %>%
    group_by(team) %>%
    mutate(mean_finish = mean(predicted_finish)) %>%
    arrange(mean_finish) %>%
    ungroup() %>%
    mutate(team = factor(team, levels = unique(team)))

  # list of team colours
  team_cols <- c(
    "red", "skyblue", "darkblue", "darkblue", "darkred",
    "orange", "red", "white", "red", "blue", "maroon",
    "blue", "white", "red", "dodgerblue", "yellow",
    "maroon", "red", "maroon", "yellow", "red", "magenta"
  )

  # plot the finishing position by chance based on these simulations
  fit_final_predicted_finishes$final_rank <- final_table_real$final_rank[match(fit_final_predicted_finishes$team, final_table_real$team)]
  p_pred_final <- ggplot(
    fit_final_predicted_finishes,
    aes(x = predicted_finish, y = perc, fill = team)
  ) +
    geom_bar(stat = "identity", colour = "black") +
    geom_vline(aes(xintercept = as.numeric(final_rank)), linetype = "dotted") +
    scale_fill_manual(values = team_cols, guide = FALSE) +
    labs(
      title = "Predicted finish position of teams",
      subtitle = "for incomplete 2019/2020 Bundesliga season",
      y = "fraction of finishes",
      x = "final position"
    ) +
    theme_minimal() +
    facet_wrap(~team, ncol = 3)

  if (show_plots) p_pred_final
}
