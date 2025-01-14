import i18next from "i18next"
import React, {memo} from "react"
import {connect} from "react-redux"
import {SwitchTransition} from "react-transition-group"
import {RootState} from "../../../reducers"
import {getFetchedProcessDetails, getProcessState, isProcessStateLoaded, getProcessUnsavedNewName, isProcessRenamed} from "../../../reducers/selectors/graph"
import {getCustomActions} from "../../../reducers/selectors/settings"
import {UnknownRecord} from "../../../types/common"
import {CssFade} from "../../CssFade"
import {descriptionProcessArchived, descriptionSubprocess, descriptionSubprocessArchived, unknownDescription} from "../../Process/messages"
import ProcessStateIcon, {unknownIcon} from "../../Process/ProcessStateIcon"
import {ProcessStateType, ProcessType} from "../../Process/types"
import {CollapsibleToolbar} from "../../toolbarComponents/CollapsibleToolbar"
import {DragHandle} from "../../toolbarComponents/DragHandle"
import {ToolbarButtons} from "../../toolbarComponents/ToolbarButtons"
import {DefaultToolbarPanel, ToolbarPanelProps} from "../../toolbarComponents/DefaultToolbarPanel"
import {ActionButton} from "../../toolbarSettings/buttons"
import UrlIcon from "../../UrlIcon"

type State = UnknownRecord

class ProcessInfo extends React.Component<ToolbarPanelProps & StateProps, State> {
  static defaultProps = {
    isStateLoaded: false,
  }

  static subprocessIcon = "/assets/process/subprocess.svg"
  static archivedIcon = "/assets/process/archived.svg"

  private getDescription = (
    process: ProcessType,
    processState: ProcessStateType,
    isStateLoaded: boolean,
  ): string => {
    return process.isArchived ?
      process.isSubprocess ?
        descriptionSubprocessArchived() :
        descriptionProcessArchived() :
      process.isSubprocess ?
        descriptionSubprocess() :
        isStateLoaded ?
          processState?.description :
          process?.state?.description || unknownDescription()
  }

  private getIcon = (
    process: ProcessType,
    processState: ProcessStateType,
    isStateLoaded: boolean,
    description: string,
  ) => {
    if (process.isArchived || process.isSubprocess) {
      const path = process.isArchived ? ProcessInfo.archivedIcon : ProcessInfo.subprocessIcon
      return (
        <UrlIcon path={path} title={description} />
      )
    }

    const size = 32
    return (
      <ProcessStateIcon
        popover={false}
        animation={false}
        process={process}
        processState={processState}
        isStateLoaded={isStateLoaded}
        height={size}
        width={size}
      />
    )
  }

  private getTransitionKey = (
    process: ProcessType,
    processState: ProcessStateType,
  ): string => process.isArchived || process.isSubprocess ?
    `${process.id}` :
    `${process.id}-${processState?.icon || process?.state?.icon || unknownIcon}`

  render() {
    const {process, processState, isStateLoaded, customActions, isRenamePending, unsavedNewName} = this.props
    const description = this.getDescription(process, processState, isStateLoaded)
    const icon = this.getIcon(process, processState, isStateLoaded, description)
    const transitionKey = this.getTransitionKey(process, processState)
    // TODO: better styling of process info toolbar in case of many custom actions
    return (
      <CollapsibleToolbar title={i18next.t("panels.status.title", "Status")} id={this.props.id}>
        <DragHandle>
          <SwitchTransition>
            <CssFade key={transitionKey}>
              <div className={"panel-process-info"}>
                <div className={"process-info-icon"}>
                  {icon}
                </div>
                <div className={"process-info-text"}>
                  {isRenamePending ?
                    (
                      <div className="process-name process-name-rename" title={process.name}>{unsavedNewName}*</div>
                    ) :
                    (
                      <div className="process-name">{process.name}</div>
                    )}
                  <div className={"process-info-description"}>{description}</div>
                </div>
              </div>
            </CssFade>
          </SwitchTransition>
          <ToolbarButtons variant={this.props.buttonsVariant}>
            {this.props.children}
            {
              //TODO: to be replaced by toolbar config
              customActions.map(action => (<ActionButton name={action.name} key={action.name}/>))
            }
          </ToolbarButtons>
        </DragHandle>
      </CollapsibleToolbar>
    )
  }
}

const mapState = (state: RootState) => ({
  isStateLoaded: isProcessStateLoaded(state),
  process: getFetchedProcessDetails(state),
  isRenamePending: isProcessRenamed(state),
  unsavedNewName: getProcessUnsavedNewName(state),
  processState: getProcessState(state),
  customActions: getCustomActions(state),
})

type StateProps = ReturnType<typeof mapState>

export default connect(mapState)(memo(ProcessInfo)) as typeof DefaultToolbarPanel

