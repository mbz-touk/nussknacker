import _ from "lodash"
import {events} from "../../analytics/TrackingEvents"
import {isEdgeEditable} from "../../common/EdgeUtils"
import * as VisualizationUrl from "../../common/VisualizationUrl"
import NodeUtils from "../../components/graph/NodeUtils"
import {DialogType} from "../../components/modals/DialogsTypes"
import history from "../../history"
import {CustomAction, Edge, NodeType} from "../../types"
import {ThunkAction} from "../reduxTypes"
import {EventInfo, reportEvent} from "./reportEvent"

export type DisplayModalNodeDetailsAction = {
  type: "DISPLAY_MODAL_NODE_DETAILS",
  nodeToDisplay: NodeType,
  nodeToDisplayReadonly: boolean,
}
export type DisplayModalEdgeDetailsAction = {
  type: "DISPLAY_MODAL_EDGE_DETAILS",
  edgeToDisplay: Edge,
}
export type ToggleModalDialogAction = {
  type: "TOGGLE_MODAL_DIALOG",
  openDialog: DialogType,
}
export type ToggleInfoModalAction = {
  type: "TOGGLE_INFO_MODAL",
  openDialog: DialogType,
  text: string,
}

export type ToggleCustomActionAction = {
  type: "TOGGLE_CUSTOM_ACTION",
  customAction: CustomAction,
}

export function displayModalNodeDetails(node: NodeType, readonly: boolean, eventInfo: EventInfo): ThunkAction {
  return (dispatch) => {
    history.replace({
      pathname: window.location.pathname,
      search: VisualizationUrl.setAndPreserveLocationParams({
        nodeId: node.id,
        edgeId: null,
      }),
    })

    !_.isEmpty(eventInfo) && dispatch(reportEvent({
      category: eventInfo.category,
      action: events.actions.buttonClick,
      name: eventInfo.name,
    }))

    return dispatch({
      type: "DISPLAY_MODAL_NODE_DETAILS",
      nodeToDisplay: node,
      nodeToDisplayReadonly: readonly,
    })
  }
}

export function displayModalEdgeDetails(edge: Edge): ThunkAction {
  return dispatch => {
    if (isEdgeEditable(edge)) {
      history.replace({
        pathname: window.location.pathname,
        search: VisualizationUrl.setAndPreserveLocationParams({
          nodeId: null,
          edgeId: NodeUtils.edgeId(edge),
        }),
      })
      return dispatch({
        type: "DISPLAY_MODAL_EDGE_DETAILS",
        edgeToDisplay: edge,
      })
    }
  }
}

export function toggleModalDialog(openDialog: DialogType): ThunkAction {
  return (dispatch) => {
    openDialog != null && dispatch(reportEvent({
      category: "right_panel",
      action: "button_click",
      name: openDialog.toLowerCase(),
    }))

    return dispatch({
      type: "TOGGLE_MODAL_DIALOG",
      openDialog: openDialog,
    })
  }
}

export function toggleInfoModal(openDialog: DialogType, text: string): ToggleInfoModalAction {
  return {
    type: "TOGGLE_INFO_MODAL",
    openDialog: openDialog,
    text: text,
  }
}

export function toggleCustomAction(customAction: CustomAction): ToggleCustomActionAction {
  return {
    type: "TOGGLE_CUSTOM_ACTION",
    customAction: customAction,
  }
}

