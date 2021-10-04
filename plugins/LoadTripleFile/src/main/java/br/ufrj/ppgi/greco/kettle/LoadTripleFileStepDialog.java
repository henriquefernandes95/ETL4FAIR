package br.ufrj.ppgi.greco.kettle;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.widget.ComboVar;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.io.*;
import java.net.URL;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;


import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.util.Models;
import org.eclipse.rdf4j.model.vocabulary.RDF;

import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.eclipse.rdf4j.repository.config.RepositoryConfigSchema;
import org.eclipse.rdf4j.repository.manager.RepositoryManager;
import org.eclipse.rdf4j.repository.manager.RepositoryProvider;
import org.eclipse.rdf4j.rio.*;

import org.eclipse.rdf4j.rio.helpers.StatementCollector;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.RDFParseException;

/**
 * Interface de usuario do step Load Triple File
 * 
 * @author Nickolas Gomes Pinto
 * 
 */
public class LoadTripleFileStepDialog extends BaseStepDialog implements StepDialogInterface {
	
	private static Class<?> PKG = LoadTripleFileStepMeta.class;
	
	private LoadTripleFileStepMeta input;
	private String dialogTitle;

	// Adicionar variaveis dos widgets
	private ComboVar wcFileFormat;
	private ComboVar wcExistsRepository;
	private TextVar wcRepoName;
	private TextVar wcGraphName;
	private TextVar wtRepoURL;
	private Label wlShape;
	private Label wlFileFormat;
	private Label wlRepoURL;
	private Label wlGraphName;
	private Label wlExistsRepository;
	private Label wlRepoName;

	private Button wbBrowse;
	private Text wBrowse;

	private FormData fdlRepoURL;
	private FormData fdtRepoURL;
	private FormData fdlRepoName;
	private FormData fdcRepoName;
	private FormData fdlGraphName;
	private FormData fdcGraphName;
	private FormData fdlExistsRepository;
	private FormData fdcExistsRepository;
	private FormData fdlFileFormat;
	private FormData fdcFileFormat;
	private FormData fdlShape;
	private FormData fdbBrowse;
	private FormData fdBrowse;

	public LoadTripleFileStepDialog(Shell parent, Object stepMeta, TransMeta transMeta, String stepname) {
		super(parent, (BaseStepMeta) stepMeta, transMeta, stepname);

		input = (LoadTripleFileStepMeta) baseStepMeta;

		// Inicializa step e seta título da caixa de interacao
		dialogTitle = BaseMessages.getString(PKG, "LoadTripleFile.Title");
	}

	public String open() {

		Shell parent = getParent();
		Display display = parent.getDisplay();

		shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
		props.setLook(shell);
		setShellImage(shell, input);


		// ModifyListener padrao
		ModifyListener lsMod = new ModifyListener() {
			public void modifyText(ModifyEvent e) {
				input.setChanged();
			}
		};

		boolean changed = input.hasChanged();

		FormLayout formLayout = new FormLayout();
		formLayout.marginWidth = Const.FORM_MARGIN;
		formLayout.marginHeight = Const.FORM_MARGIN;

		shell.setLayout(formLayout);

		shell.setText(dialogTitle);

		int middle = props.getMiddlePct();
		int margin = Const.MARGIN;

		// Campo nome do Step
		wlStepname = new Label(shell, SWT.RIGHT);
		wlStepname.setText(BaseMessages.getString(PKG, "LoadTripleFile.StepNameField.Label"));
		props.setLook(wlStepname);
		fdlStepname = new FormData();
		fdlStepname.left = new FormAttachment(0, 0);
		fdlStepname.right = new FormAttachment(middle, -margin);
		fdlStepname.top = new FormAttachment(0, margin);
		wlStepname.setLayoutData(fdlStepname);

		wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		wStepname.setText(stepname);
		props.setLook(wStepname);
		wStepname.addModifyListener(lsMod);
		fdStepname = new FormData();
		fdStepname.left = new FormAttachment(middle, 0);
		fdStepname.top = new FormAttachment(0, margin);
		fdStepname.right = new FormAttachment(100, 0);
		wStepname.setLayoutData(fdStepname);

		// Campo com URL do Banco em Grafos
		wlRepoURL = new Label(shell, SWT.RIGHT);
		wlRepoURL.setText(BaseMessages.getString(PKG, "LoadTripleFile.RepoURLField.Label")); 
		props.setLook(wlRepoURL);
		fdlRepoURL = new FormData();
		fdlRepoURL.left = new FormAttachment(0, 0);
		fdlRepoURL.top = new FormAttachment(wStepname, margin);
		fdlRepoURL.right = new FormAttachment(middle, -margin);
		wlRepoURL.setLayoutData(fdlRepoURL);

		wtRepoURL = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		wtRepoURL.setText(""); 
		props.setLook(wtRepoURL);
		wtRepoURL.addModifyListener(lsMod);
		fdtRepoURL = new FormData();
		fdtRepoURL.left = new FormAttachment(middle, 0);
		fdtRepoURL.right = new FormAttachment(100, 0);
		fdtRepoURL.top = new FormAttachment(wStepname, margin);
		wtRepoURL.setLayoutData(fdtRepoURL);

		// Adiciona label e combo do campo para verificacao de uso de repositório existente
		wlExistsRepository = new Label(shell, SWT.RIGHT);
		wlExistsRepository.setText(BaseMessages.getString(PKG, "LoadTripleFile.ExistsRepositoryField.Label"));
		props.setLook(wlExistsRepository);
		fdlExistsRepository = new FormData();
		fdlExistsRepository.left = new FormAttachment(0, 0);
		fdlExistsRepository.top = new FormAttachment(wtRepoURL, margin);
		fdlExistsRepository.right = new FormAttachment(middle, -margin);
		wlExistsRepository.setLayoutData(fdlExistsRepository);

		wcExistsRepository = new ComboVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		wcExistsRepository.setItems( new String[]{ "S", "N" } );
		wcExistsRepository.setEditable( false );
		wcExistsRepository.select( 0 );
		props.setLook(wcExistsRepository);
		wcExistsRepository.addModifyListener(lsMod);
		fdcExistsRepository = new FormData();
		fdcExistsRepository.left = new FormAttachment(middle, 0);
		fdcExistsRepository.right = new FormAttachment(100, 0);
		fdcExistsRepository.top = new FormAttachment(wtRepoURL, margin);
		wcExistsRepository.setLayoutData(fdcExistsRepository);

		// Adiciona label e combo do campo do nome do repositório
		wlRepoName = new Label(shell, SWT.RIGHT);
		wlRepoName.setText(BaseMessages.getString(PKG, "LoadTripleFile.RepoNameField.Label"));
		props.setLook(wlRepoName);
		fdlRepoName = new FormData();
		fdlRepoName.left = new FormAttachment(0, 0);
		fdlRepoName.right = new FormAttachment(middle, -margin);
		fdlRepoName.top = new FormAttachment(wcExistsRepository, margin);
		wlRepoName.setLayoutData(fdlRepoName);

		wcRepoName = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		wtRepoURL.setText(""); 
		props.setLook(wcRepoName);
		wcRepoName.addModifyListener(lsMod);
		fdcRepoName = new FormData();
		fdcRepoName.left = new FormAttachment(middle, 0);
		fdcRepoName.top = new FormAttachment(wcExistsRepository, margin);
		fdcRepoName.right = new FormAttachment(100, 0);
		wcRepoName.setLayoutData(fdcRepoName);

		// Adiciona label e combo do campo de selecao do formato do arquivo
		wlFileFormat = new Label(shell, SWT.RIGHT);
		wlFileFormat.setText(BaseMessages.getString(PKG, "LoadTripleFile.FileFormatField.Label"));
		props.setLook(wlFileFormat);
		fdlFileFormat = new FormData();
		fdlFileFormat.left = new FormAttachment(0, 0);
		fdlFileFormat.top = new FormAttachment(wcRepoName, margin);
		fdlFileFormat.right = new FormAttachment(middle, -margin);
		wlFileFormat.setLayoutData(fdlFileFormat);

		wcFileFormat = new ComboVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		wcFileFormat.setItems( new String[]{ "TURTLE", "RDFXML", "RDFJSON", "N3", "NTRIPLES", "NQUAD", "TRIG", "TRIX", "JSONLD" } );
		wcFileFormat.setEditable( false );
		wcFileFormat.select( 0 );
		props.setLook(wcFileFormat);
		wcFileFormat.addModifyListener(lsMod);
		FormData fdcFileFormat = new FormData();
		fdcFileFormat.left = new FormAttachment(middle, 0);
		fdcFileFormat.right = new FormAttachment(100, 0);
		fdcFileFormat.top = new FormAttachment(wcRepoName, margin);
		wcFileFormat.setLayoutData(fdcFileFormat);


		// Bottom buttons
		wOK = new Button(shell, SWT.PUSH);
		wOK.setText(BaseMessages.getString(PKG, "LoadTripleFile.Btn.OK")); //$NON-NLS-1$
		wCancel = new Button(shell, SWT.PUSH);
		wCancel.setText(BaseMessages.getString(PKG, "LoadTripleFile.Btn.Cancel")); //$NON-NLS-1$
		setButtonPositions(new Button[] { wOK, wCancel }, margin, wBrowse);

		wlShape = new Label(shell, SWT.RIGHT);
		wlShape.setText(BaseMessages.getString(PKG, "LoadTripleFile.MappingFile.Label"));
		props.setLook(wlShape);
		fdlShape = new FormData();
		fdlShape.left = new FormAttachment(0, 0);
		fdlShape.top = new FormAttachment(wcFileFormat, margin);
		fdlShape.right = new FormAttachment(middle, -margin);
		wlShape.setLayoutData(fdlShape);

		// Botoes para busca de arquivo
		wbBrowse = new Button(shell, SWT.PUSH | SWT.CENTER);
		props.setLook(wbBrowse);
		wbBrowse.setText(BaseMessages.getString(PKG, "LoadTripleFile.Btn.Browse"));
		fdbBrowse = new FormData();
		fdbBrowse.right = new FormAttachment(100, 0);
		fdbBrowse.top = new FormAttachment(wcFileFormat, margin);
		wbBrowse.setLayoutData(fdbBrowse);

		wBrowse = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wBrowse);
		wBrowse.addModifyListener(lsMod);
		fdBrowse = new FormData();
		fdBrowse.left = new FormAttachment(middle, 0);
		fdBrowse.right = new FormAttachment(wbBrowse, -margin);
		fdBrowse.top = new FormAttachment(wcFileFormat, margin);
		wBrowse.setLayoutData(fdBrowse);

		// Add listeners
		lsCancel = new Listener() {
			public void handleEvent(Event e) {
				cancel();
			}
		};
		lsOK = new Listener() {
			public void handleEvent(Event e) {
				ok();
			}
		};

		wBrowse.addModifyListener(new ModifyListener() {
			public void modifyText(ModifyEvent arg0) {
				wBrowse.setToolTipText(transMeta.environmentSubstitute(wBrowse.getText()));
			}
		});

		wbBrowse.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(SelectionEvent e) {
				FileDialog dialog = new FileDialog(shell, SWT.OPEN);
				dialog.setFilterExtensions(new String[] { "*.rdf;*.n3;*.ttl;*.xml;*owl;*.rdfs;*.rj;*.nt;*.nq;*.trig;*.trix;*.jsonld", "*" });
				if (wBrowse.getText() != null) {
					dialog.setFileName(wBrowse.getText());
				}

				dialog.setFilterNames(new String[] { "RDF Files", "All files" });

				if (dialog.open() != null) {
					String str = dialog.getFilterPath() + System.getProperty("file.separator") + dialog.getFileName();
					wBrowse.setText(str);
				}
			}
		});

		// Adiciona label e combo do campo do nome do grafo
		wlGraphName = new Label(shell, SWT.RIGHT);
		wlGraphName.setText(BaseMessages.getString(PKG, "LoadTripleFile.GraphName.Label"));
		props.setLook(wlGraphName);
		fdlGraphName = new FormData();
		fdlGraphName.left = new FormAttachment(0, 0);
		fdlGraphName.right = new FormAttachment(middle, -margin);
		fdlGraphName.top = new FormAttachment(wBrowse, margin);
		wlGraphName.setLayoutData(fdlGraphName);

		wcGraphName = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		wtRepoURL.setText(""); 
		props.setLook(wcGraphName);
		wcGraphName.addModifyListener(lsMod);
		fdcGraphName = new FormData();
		fdcGraphName.left = new FormAttachment(middle, 0);
		fdcGraphName.top = new FormAttachment(wBrowse, margin);
		fdcGraphName.right = new FormAttachment(100, 0);
		wcGraphName.setLayoutData(fdcGraphName);

		wCancel.addListener(SWT.Selection, lsCancel);
		wOK.addListener(SWT.Selection, lsOK);

		// Fecha a janela em caso afirmativo para qualquer um dos inputs
		lsDef = new SelectionAdapter() {
			public void widgetDefaultSelected(SelectionEvent e) {
				ok();
			}
		};
		wStepname.addSelectionListener(lsDef);

		// Listener para delete fechar pagina
		shell.addShellListener(new ShellAdapter() {
			public void shellClosed(ShellEvent e) {
				cancel();
			}
		});

		// Popula data dos controles criados
		getData();

		// Seta tamanho do shell
		setSize();

		// Alarga um pouco mais a janela
		Rectangle shellBounds = shell.getBounds();
		shellBounds.width += 5;
		shellBounds.height += 5;
		shell.setBounds(shellBounds);

		input.setChanged(changed);

		shell.open();
		while (!shell.isDisposed()) {
			if (!display.readAndDispatch())
				display.sleep();
		}
		return stepname;
	}

	private void getData() {
		wStepname.selectAll();

		wcFileFormat.setText(Const.NVL(input.getInputFileFormat(), ""));
		wcExistsRepository.setText(Const.NVL(input.getExistsRepository(), ""));
		wcRepoName.setText(Const.NVL(input.getInputRepoName(), ""));
		wcGraphName.setText(Const.NVL(input.getInputGraph(), ""));
		wtRepoURL.setText(Const.NVL(input.getInputRepoURL(), ""));
		wBrowse.setText(input.getBrowseFilename());
	}

	protected void cancel() {
		stepname = null;
		input.setChanged(changed);
		dispose();
	}

	protected void ok() {
		if (StringUtil.isEmpty(wStepname.getText()))
			return;

		stepname = wStepname.getText(); // return value

		// Pegar dados da GUI e colocar no StepMeta
		input.setInputFileFormat(wcFileFormat.getText());
		input.setExistsRepository(wcExistsRepository.getText());
		input.setInputRepoName(wcRepoName.getText());
		input.setInputGraph(wcGraphName.getText());
		input.setInputRepoURL(wtRepoURL.getText());
		input.setBrowseFilename(wBrowse.getText());

		// Fecha janela
		dispose();
	}
}
