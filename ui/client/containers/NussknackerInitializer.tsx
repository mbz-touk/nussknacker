import React, {PropsWithChildren} from "react"
import {useDispatch} from "react-redux"
import {assignUser} from "../actions/nk"
import HttpService from "../http/HttpService"
import {AuthInitializer} from "./Auth"

function NussknackerInitializer({children}: PropsWithChildren<unknown>): JSX.Element {
  const dispatch = useDispatch()

  const onAuth = () => HttpService.fetchLoggedUser().then(({data}) => {
    console.log("dispatching user data", assignUser(data))
    dispatch(assignUser(data))
  })

  return (
    <AuthInitializer onAuthFulfilled={onAuth}>
      {children}
    </AuthInitializer>
  )
}

export default NussknackerInitializer
