import {DialogType, dialogTypesMap} from "../components/modals/DialogsTypes"
import {CustomAction, ProcessId} from "../types"
import {Reducer} from "../actions/reduxTypes"
import {mergeReducers} from "./mergeReducers"

export type UiState = {
  showNodeDetailsModal: boolean,
  showEdgeDetailsModal: boolean,
  confirmDialog: Partial<{
    isOpen: boolean,
    text: string,
    confirmText: string,
    denyText: string,
    onConfirmCallback: () => void,
  }>,
  modalDialog: Partial<{
    openDialog: DialogType,
    message: string,
    action: (processId: ProcessId, comment: string) => void,
    displayWarnings: boolean,
    text: string,
    customAction: CustomAction,
  }>,
  isToolTipsHighlighted: boolean,
}

const emptyUiState: UiState = {
  showNodeDetailsModal: false,
  showEdgeDetailsModal: false,
  isToolTipsHighlighted: false,
  confirmDialog: {},
  modalDialog: {},
}

const uiReducer: Reducer<UiState> = (state = emptyUiState, action) => {
  switch (action.type) {
    case "SWITCH_TOOL_TIPS_HIGHLIGHT": {
      return {
        ...state,
        isToolTipsHighlighted: action.isHighlighted,
      }
    }
    case "CLEAR":
    case "CLOSE_MODALS": {
      return {
        ...state,
        showNodeDetailsModal: false,
        showEdgeDetailsModal: false,
      }
    }
    case "DISPLAY_MODAL_NODE_DETAILS": {
      return {
        ...state,
        showNodeDetailsModal: true,
      }
    }
    case "DISPLAY_MODAL_EDGE_DETAILS": {
      return {
        ...state,
        showEdgeDetailsModal: true,
      }
    }
    case "TOGGLE_CONFIRM_DIALOG": {
      return {
        ...state,
        confirmDialog: {
          isOpen: action.isOpen,
          text: action.text,
          confirmText: action.confirmText,
          denyText: action.denyText,
          onConfirmCallback: action.onConfirmCallback,
        },
      }
    }
    case "TOGGLE_MODAL_DIALOG": {
      return {
        ...state,
        modalDialog: {
          openDialog: action.openDialog,
        },
      }
    }
    case "TOGGLE_INFO_MODAL": {
      return {
        ...state,
        modalDialog: {
          openDialog: action.openDialog,
          text: action.text,
        },
      }
    }
    case "TOGGLE_PROCESS_ACTION_MODAL": {
      return {
        ...state,
        modalDialog: {
          openDialog: dialogTypesMap.processAction,
          message: action.message,
          action: action.action,
          displayWarnings: action.displayWarnings,
        },
      }
    }
    case "TOGGLE_CUSTOM_ACTION": {
      return {
        ...state,
        modalDialog: {
          openDialog: dialogTypesMap.customAction,
          customAction: action.customAction,
        },
      }
    }

    default:
      return state
  }
}

export const reducer = mergeReducers<UiState>(
  uiReducer,
)
