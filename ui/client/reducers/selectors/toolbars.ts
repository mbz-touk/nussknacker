import {defaultToolbarsConfig} from "../../components/toolbarSettings/defaultToolbarsConfig"
import {RootState} from "../index"
import {ToolbarsSide, ToolbarsState, ToolbarsStates} from "../toolbars"
import {createSelector} from "reselect"
import {getSettings} from "./settings"
import {useSelector} from "react-redux"
import {isArchived, isSubprocess} from "./graph"

const getToolbarsState = (state: RootState): ToolbarsStates => state.toolbars || {}
export const getToolbars = createSelector(getToolbarsState, (t): ToolbarsState => {
  return t[`#${t.currentConfigId}`] || {}
})
export const getToolbarsInitData = createSelector(getToolbars, t => t.initData || [])
export const getPositions = createSelector(getToolbars, t => t.positions || {})

export const getNodeToolbox = createSelector(getToolbars, t => t.nodeToolbox)
export const getOpenedNodeGroups = createSelector(getNodeToolbox, t => t?.opened || {})

const getCollapsed = createSelector(getToolbars, t => t.collapsed)

export const getIsCollapsed = createSelector(getCollapsed, collapsed => (id: string) => !!collapsed[id])
export const getOrderForPosition = (side: ToolbarsSide) => (state: RootState) => getPositions(state)[side] || []

export const getToolbarsConfig = createSelector(getSettings, isSubprocess, isArchived, (settings, subprocess, archived) =>
  settings?.processToolbarsConfiguration || defaultToolbarsConfig(subprocess, archived)
)

export const isLeftPanelOpened = createSelector(getToolbars, ({panels}) => panels?.left)
export const isRightPanelOpened = createSelector(getToolbars, ({panels}) => panels?.right)
