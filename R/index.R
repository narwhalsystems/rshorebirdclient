library(ghql)
library(magrittr)
library(dplyr)
library(anytime)


NarwhalClient <- R6::R6Class(
  "NarwhalClient",
  portable = TRUE,
  cloneable = FALSE,
  private = list(
    graphqlClient = NULL
  ),
  public = list(
    token = NULL,
    initialize = function(token) {

      if (missing(token)) {
        stop(sprintf("NarwhalClient requires a token!"), call. = FALSE)
      }
      private$graphqlClient <- GraphqlClient$new(
        url = "https://domain-shorebirds.userdata.staging.narwhal.sh/graphql",
        headers = list(Authorization = paste0("Token ", token))
      )
    },
    get_nests = function(limit=999999999) {
      composed_query_string = paste(
        'query FetchUserdata(
    $limit: Int
) {
    get_userdata(
        query: {
            collection: "shorebirds.nest"
            limit: $limit
            filter: ',
        '{}',
        '
        }
        joins: [
          {
              name: "checks"
              collection: "shorebirds.check"
              local_path: "global_id"
              foreign_path: "nest_id"
              count: false
          }
          {
              name: "region"
              collection: "core.region"
              local_path: "sdzg_int.region_id"
              foreign_path: "global_id"
              count: false
              toOne: true
          }
          {
              name: "chicks"
              collection: "shorebirds.bird"
              local_path: "global_id"
              foreign_path: "origin_nest_id"
              count: false
              toOne: false
          }
          {
              name: "eggs"
              collection: "shorebirds.egg"
              local_path: "_join/checks/global_id"
              foreign_path: "check_id"
              count: false
              toOne: false
          }
          {
              name: "encounters"
              collection: "shorebirds.encounter"
              local_path: "global_id"
              foreign_path: "nest_id"
              count: false
              toOne: false
          }
        ]
    ) {
        data {
            user_datum {
                data
                user_id
                group_id
            }
            joins
        }
        cursor
    }
}')

      qry <- Query$new()
      qry$query('mydata', composed_query_string)

      args <- list( limit=limit)
      res <- private$graphqlClient$exec(qry$queries$mydata, args)
      parsed <- jsonlite::fromJSON(res)
      return_me <- parsed$data$get_userdata$data$user_datum$data
      return_me$region <- parsed$data$get_userdata$data$joins$region$name
      return_me$checks <- parsed$data$get_userdata$data$joins$checks
      return_me$eggs <- parsed$data$get_userdata$data$joins$eggs
      return_me$chicks <- parsed$data$get_userdata$data$joins$chicks
      # return_me$encounters <- parsed$data$get_userdata$data$joins$encounters
      mutated <- return_me %>% mutate(
        date_discovered = anytime(date_discovered/1000),
        date_closed = anytime(date_closed/1000),
        date_time_created = NULL,
        populations = NULL,
        sdzg_int.region_id = NULL
      )
      return(mutated)

    }
  )
)
